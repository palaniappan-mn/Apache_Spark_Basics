#write results to a hive table
#saveAsTable, insertInto
ordersDF.
    write. \
    format('hive'). \
    mode('overwrite'). \
    saveAsTable("orders",mode="overwrite")

ordersDF. \
    write. \
    format('hive'). \
    insertInto("orders")

#write the dataframe results to a hdfs location
ordersDF. \
    write. \
    format('csv'). \
    save('/user/palaniappan_mn/practice/orders_csv')

ordersDF.write. \
  format('json'). \
  save('/user/training/bootcampdemo/pyspark/orders_json')

ordersDF. \
    write. \
    parquet('/user/palaniappan_mn/practice/orders_csv')

#read from relational databases
orders = spark.read \
    .format('jdbc') \
    .option('url','jdbc:mysql://ms.itversity.com') \
    .option('dbtable','retail_db.orders') \
    .option('user','retail_user') \
    .option('password','itversity') \
    .load()

#HDFS commands using os.system
os.system('hdfs dfs -ls "/user/palaniappan_mn/practice"')
os.system('hdfs dfs -cat "/user/palaniappan_mn/practice/part-00000-761f9850-016c-4b03-aa0b-60ad8f503908-c000.csv" | head -n 5') #see the first 5 rows
os.system('hdfs dfs -rm -r  "/user/palaniappan_mn/practice/orders_csv"') #removes a non empty directory recursively


ordersDF.write. \
     format('csv'). \
     option('delimiter',','). \
     option('compression','gzip')
     mode('overwrite'). \
     save('/user/palaniappan_mn/practice')
