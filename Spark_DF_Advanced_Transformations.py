from pyspark.sql.window import *

#Find the pecentage of each order_item price in an order_id
window_spec = Window.partitionBy(ordersItemsDF.order_item_order_id)

ordersItemsDF. \
    select('order_item_order_id',
           'order_item_id',
           round(ordersItemsDF.order_item_subtotal
                /sum('order_item_subtotal').over(window_spec),2). \
           alias('order_item_revenue_percentage')). \
    show(5)

#Get average revenue for each day and get all the orders who earn revenue more than average revenue
order_subtotal_spec = Window.partitionBy(ordersDF.order_id)
avg_revenue_spec = Window.partitionBy(ordersDF.order_date)

ordersItemsDF. \
    join(ordersDF, ordersItemsDF.order_item_order_id == ordersDF.order_id). \
    select('order_date',
           'order_id',
           round(sum('order_item_subtotal').over(order_subtotal_spec),2).alias('order_subtotal'),
           round(avg('order_item_subtotal').over(avg_revenue_spec),2).alias('avg_revenue_per_day')). \
    filter('order_subtotal > avg_revenue_per_day'). \
    dropDuplicates(). \
    show()

#Get highest order revenue per day and get all the orders which have revenue more than 75% of the revenue
order_subtotal_spec = Window.partitionBy(ordersDF.order_id)
max_revenue_spec = Window.partitionBy(ordersDF.order_date)

temp_table = ordersItemsDF. \
    join(ordersDF, ordersItemsDF.order_item_order_id == ordersDF.order_id). \
    select('order_date',
           'order_id',
           round(sum('order_item_subtotal').over(order_subtotal_spec),2).alias('order_subtotal'))

temp_table. \
    withColumn('max_order_revenue_per_day',max('order_subtotal').over(avg_revenue_spec)). \
    filter('order_subtotal >= 0.75 * max_order_revenue_per_day'). \
    show()
