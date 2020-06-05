#Window functions
#Get top 3 Products by revenue Per day
spark.sql('''select order_date, product_id, product_name, rnk as revenue_rank, daily_product_revenue
             from (select order_date, order_item_product_id, round(sum(order_item_subtotal),2) as daily_product_revenue,
                           dense_rank() over(partition by order_date order by sum(order_item_subtotal) desc) as rnk
                   from orders o join order_items oi
                     on o.order_id = order_item_order_id
                   group by order_date, order_item_product_id) t join products p
               on t.order_item_product_id = p.product_id
              where rnk <= 3
          ''').show()

#Get average salary for each department and get all employee details who earn more than average salary
spark.sql('''select emp_name, dept_id, avg_dept_salary
             from (select emp_name, dept_id, avg(emp_salary) over(partition by dept_id) avg_dept_salary
                   from employees)
             where emp_salary > avg_dept_salary
          ''').show()

#Get average revenue for each day and get all the orders who earn revenue more than average revenue
spark.sql('''select distinct order_date, order_id, revenue_per_order, avg_daily_revenue
             from (select order_id,
                        order_date,
                        round(sum(order_item_subtotal) over(partition by order_item_order_id),2) revenue_per_order,
                        round(avg(order_item_subtotal) over(partition by order_date),2) avg_daily_revenue
                   from order_items join orders
                     on order_id = order_item_order_id) t
             where revenue_per_order > avg_daily_revenue
          ''').show()

#Get highest order revenue and get all the orders which have revenue more than 75% of the revenue
