from pyspark.sql.types import IntegerType, FloatType

#ssh palaniappan_mn@gw02.itversity.com
#itversity password: ieXaedoh0cevoojoquu4kiemaiz7yee7

#Initiate spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("PySpark Cert Preparation") \
    .getOrCreate()

pyspark --master yarn --conf spark.ui.port=12905

#reading and converting data type
#read in orders Data
ordersDF = spark.read \
    .format('csv') \
    .schema('''order_id int,
               order_renamed_date string,
               customer_id int,
               order_status string
            ''') \
    .load('/user/palaniappan_mn/retail_db/orders')

#read in order_items data
ordersItemsDF = spark.read \
    .format('csv') \
    .schema('''order_item_id int,
               order_item_order_id int,
               order_item_product_id int,
               order_item_quantity int,
               order_item_subtotal float,
               order_item_product_price float
            ''') \
    .load('/user/palaniappan_mn/retail_db/order_items')

#read in products data
productsDF = spark.read \
    .format('csv') \
    .schema('''product_id int,
               product_category_id int,
               product_name string,
               product_description string,
               product_price float,
               product_image string
            ''') \
    .load('/user/palaniappan_mn/retail_db/products')

#check the schema
ordersItemsDF.printSchema()

#check the preview of the table
ordersItemsDF.show(5)

#read data from hive tables
ordersDF = spark.read.table("bootcampdemo.orders_hive")

#read from relational databases
orders = spark.read \
    .format('jdbc') \
    .option('url','jdbc:mysql://ms.itversity.com') \
    .option('dbtable','retail_db.orders') \
    .option('user','retail_user') \
    .option('password','itversity') \
    .load()

#alternate approach to read data from RDBMS
orders = spark.read \
    .jdbc('jdbc:mysql://ms.itversity.com',
          'retail_db.orders',
          numPartitions=4,
          column='order_id',
          lowerBound='10000',
          upperBound='20000'
          properties={'user':'retail_db','password':'itversity'})

#use a query to import data to spark from RDBMS
orders = spark.read \
    .jdbc('jdbc:mysql://ms.itversity.com',
          '(select order_items_order_id, sum(order_item_subtotal) order_revenue '
          'from retail_db.order_items group by order_item_order_id) q',
          properties={'user':'retail_user','password':'iteversity'})
