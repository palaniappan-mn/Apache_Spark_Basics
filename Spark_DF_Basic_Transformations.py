from pyspark.sql.functions import *
from pyspark.sql.types import *

#substring function
ordersDF. \
    select(ordersDF.order_date.substr(1,7).alias('order_month')).show()
ordersDF. \
    select(substring(ordersDF.order_date,1,7).alias('order_month')).show()

#lower function
ordersDF. \
    select(ordersDF.order_id,
           lower(ordersDF.order_status).alias('lower_status')).show()

#selectExpr projects a set of SQL expressions
ordersDF.selectExpr('date_format(order_date, "YYYYMM") as date_month').show()

#filter is an alias to where and both the functions filter the data
#Get orders which are either COMPLETE or CLOSED
ordersDF.filter("order_status = 'COMPLETE' or order_status = 'CLOSED'").show()
ordersDF.filter("order_status in ('COMPLETE','CLOSED')").show()
ordersDF.filter((ordersDF.order_status=='COMPLETE').__or__(ordersDF.order_status=='CLOSED')).show()
ordersDF.filter((ordersDF.order_status=='COMPLETE')|(ordersDF.order_status=='CLOSED')).show()
ordersDF.filter(ordersDF.order_status.isin('COMPLETE','CLOSED')).show()

#Get orders which are either COMPLETE or CLOSED and placed in month of 2013 August
ordersDF.filter("order_status in ('COMPLETE','CLOSED') and order_date like '2013-08%'").show()
ordersDF.filter("order_status in ('COMPLETE','CLOSED') and date_format(order_date,'YYYY-MM') = '2013-08'").show()
ordersDF.filter(ordersDF.order_status.isin('COMPLETE','CLOSED').__and__(ordersDF.order_date.like('2013-08%'))).show()

#Get order items where order_item_subtotal is not equal to product of order_item_quantity and order_item_product_price
ordersItemsDF.filter("order_item_subtotal != round(order_item_quanity * order_item_product_price,2)").show(5)
ordersItemsDF. \
    filter(ordersItemsDF.order_item_subtotal !=
        round((ordersItemsDF.order_item_quantity * ordersItemsDF.order_item_product_price),2)).show()

#Get all the orders which are placed on first of every month
#use 'dd' to get the day and not 'DD'
ordersDF.filter("date_format(order_date,'dd') = '01'").show()

#JOINS
#dataframe.join(other_dataframe, dataframe1_column == dataframe2_column, [inner,left,right,outer])
#Get all the order items corresponding to COMPLETE or CLOSED orders
ordersDF.filter("order_status in ('COMPLETE','CLOSED')"). \
    join(ordersItemsDF, ordersDF.order_id == ordersItemsDF.order_item_order_id). \
    select(ordersItemsDF.order_item_id,
        ordersItemsDF.order_item_order_id,
        ordersItemsDF.order_item_product_id,
        ordersItemsDF.order_item_quantity,
        ordersItemsDF.order_item_subtotal,
        ordersItemsDF.order_item_product_price). \
    show()

#Get all the orders where there are no corresponding order_items
ordersDF. \
    join(ordersItemsDF, ordersDF.order_id == ordersItemsDF.order_item_order_id, 'left'). \
    filter(ordersItemsDF.order_item_id.isNull()). \
    show()

#Check if there are any order_items where there is no corresponding order in orders data set
ordersDF. \
    join(ordersItemsDF, ordersDF.order_id ==ordersItemsDF.order_item_order_id, 'right'). \
    filter('order_id is null'). \
    show()

#groupBy(*cols) groups the column by which we need to aggregate
#agg(*expressions) defines the operation on the grouped data
#Get count by status from orders
ordersDF. \
    groupBy('order_status'). \
    agg(count('order_id').alias('No_of_records')). \
    show()

#Get revenue for each order id from order items
ordersItemsDF. \
    groupBy('order_item_order_id'). \
    agg(round(sum('order_item_subtotal'),2).alias('Total_revenue_by_order_id')). \
    show()

#Get daily product revenue (order_date and order_item_product_id are part of keys, order_item_subtotal is used for aggregation)
ordersDF. \
    join(ordersItemsDF, ordersDF.order_id == ordersItemsDF.order_item_order_id). \
    groupBy(ordersDF.order_date,ordersItemsDF.order_item_product_id). \
    agg(round(sum(ordersItemsDF.order_item_subtotal),2).alias('Daily_Product_Revenue')). \
    orderBy(ordersDF.order_date, ordersItemsDF.order_item_product_id). \
    show()

#sorting the result
#sort and orderBy are aliases
#sort orders by status
ordersDF.sort('order_status').show()

#Sort orders by date and then by status
ordersDF.sort(['order_date','order_status']).show()

#Sort order items by order_item_order_id and order_item_subtotal descending
ordersItemsDF.sort(['order_item_order_id','order_item_subtotal'],ascending=[1,0]).show()

#Take daily product revenue data and sort in ascending order by date and then descending order by revenue.
ordersDF. \
    join(ordersItemsDF, ordersDF.order_id == ordersItemsDF.order_item_order_id). \
    groupBy(ordersDF.order_date,ordersItemsDF.order_item_product_id). \
    agg(round(sum(ordersItemsDF.order_item_subtotal),2).alias('Daily_Product_Revenue')). \
    orderBy('order_date', desc('Daily_Product_Revenue')). \
    show()

#Exercises
#Get number of closed or complete orders placed by each customer
ordersDF. \
    filter('order_status in ("COMPLETE","CLOSED")'). \
    groupBy('customer_id'). \
    agg(count('order_id').alias('Total_Closed_Complete_orders')). \
    show()

#Get revenue generated by each customer for the month of 2014 January
#(consider only closed or complete orders)
ordersDF. \
    filter('order_status in ("COMPLETE","CLOSED") and date_format(order_date,"YYYY-MM")="2014-01"'). \
    join(ordersItemsDF, ordersDF.order_id == ordersItemsDF.order_item_order_id). \
    groupBy('customer_id'). \
    agg(round(sum('order_item_subtotal'),2).alias('Revenue_by_customer_id')). \
    show()

#Get revenue generated by each product on monthly basis –
#get product name, month and revenue generated by each product (round off revenue to 2 decimals)



#Get revenue generated by each product category on daily basis –
#get category name, date and revenue generated by each category (round off revenue to 2 decimals)



#Get the details of the customers who never placed orders
