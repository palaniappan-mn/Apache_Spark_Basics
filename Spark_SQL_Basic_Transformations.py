#dataframe has to be registered as temp tables to run sql queries
#dataframe.createTempView(name)
#dataframe.createOrReplaceTempView(name)
#dataframe.createTable(name, path, source, schema)

ordersDF.createTempView('orders')
ordersItemsDF.createTempView('order_items')
productsDF.createTempView('products')

#The below command will list all the tables in the default schema
spark.sql("show tables")

#create a table in Hive using spark
#Hive tables are nothing but HDFS directories
spark.sql('create table as orders_hive as select * from orders')

#preview data from the temo view
spark.sql('select * from orders').show(5)
spark.sql('select * from order_items').show(5)

#filtering data
#get all the data from orders which have 'COMPLETE' or 'CLOSED'
spark.sql('''select *
             from orders
             where order_status in ("COMPLETE","CLOSED")
          ''').show()

#Get orders which are either COMPLETE or CLOSED and placed in month of 2013 August
spark.sql('''select *
             from orders
             where order_status in ("COMPLETE","CLOSED")
             and order_date like '2013-08%'
          ''').show()

#Get order items where order_item_subtotal is not equal to product of order_item_quantity and order_item_product_price
spark.sql('''select *
             from order_items
             where order_item_subtotal != round(order_item_quantity * order_item_product_price,2)
          ''').show()

#Get all the orders which are placed on first of every month
spark.sql('''select *
             from orders
             where date_format(order_date,'dd') = 01
             and order_status in ('COMPLETE','CLOSED')
          ''').show()

#Joins
#Get all the order items corresponding to COMPLETE or CLOSED orders
spark.sql('''select oi.*
             from (select order_id from orders where order_status in ("COMPLETE","CLOSED")) o
             inner join order_items oi
             on o.order_id = oi.order_item_order_id
          ''').show()

#Get all the orders where there are no corresponding order_items
spark.sql('''select o.*
             from orders o left join order_items oi
             on o.order_id = oi.order_item_order_id
             where oi.order_item_order_id is null
          ''').show()

#Check if there are any order_items where there is no corresponding order in orders data set
spark.sql('''select oi.*
             from orders o right join order_items oi
             on o.order_id = oi.order_item_order_id
             where o.order_id is null
          ''').show()

#aggregations using groupBy and functions
#Get count by status from orders
spark.sql('''select order_status, count(*) as order_count
             from orders
             group by order_status
          ''').show()

#Get revenue for each order id from order items
spark.sql('''select order_item_order_id, round(sum(order_item_subtotal),2) as order_revenue
             from order_items
             group by order_item_order_id
          ''').show()

#Get daily product revenue
#(order_date and order_item_product_id are part of keys, order_item_subtotal is used for aggregation)
spark.sql('''select order_date, order_item_product_id, round(sum(order_item_subtotal),2) as daily_product_revenue
             from orders o join order_items oi
             on o.order_id = oi.order_item_order_id
             group by order_date, order_item_product_id
          ''').show()

#sorting data using order by clause
#Sort orders by status
spark.sql('''select *
             from orders
             order by order_status
          ''').show()

#Sort orders by date and then by status
spark.sql('''select *
             from orders
             order by order_date, order_status
          ''').show()

#Sort order items by order_item_order_id and order_item_subtotal descending
spark.sql('''select *
             from order_items
             order by order_item_order_id, order_item_subtotal desc
          ''').show()

#Take daily product revenue data and sort in ascending order by date and then descending order by revenue.
spark.sql('''select order_date, order_item_product_id, round(sum(order_item_subtotal),2) as daily_product_revenue
             from orders o join order_items oi
             on o.order_id = oi.order_item_order_id
             group by order_date, order_item_product_id
             order by order_date, daily_product_revenue desc
          ''').show()
