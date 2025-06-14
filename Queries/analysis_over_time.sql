select
datetrunc(month, order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customer, 
sum(quantity) as total_quantity,
rank() over(order by sum(sales_amount) desc) as ranking
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date)

--2013 had significantly higher sales but dropped in Jan 2014


select
order_date,
total_sales,
sum(total_sales) over(order by order_date) as running_total,
avg(avg_price) over(order by order_date) as moving_avg_price
from (
select
datetrunc(YEAR, order_date) as order_date,
sum(sales_amount) as total_sales,
AVG(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(YEAR, order_date)
) t 

/* total sales increased when the average price decreased
so I suggest focusing on developing pricing strategies */

	
with yearly_product_sales as (
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as currnet_sales
from gold.fact_sales as f
left join gold.dim_products as p 
on f.product_key = p.product_key
where f.order_date is not null 
group by year(f.order_date), p.product_name )

select 
order_year,
product_name,
currnet_sales,
avg(currnet_sales) over(partition by product_name) as avg_sales,
currnet_sales - avg(currnet_sales) over(partition by product_name) as diff_avg,
case when currnet_sales - avg(currnet_sales) over(partition by product_name) > 0 then 'above avg'
	 when currnet_sales - avg(currnet_sales) over(partition by product_name) < 0 then 'below avg'
	 else 'same' end avg_change,
lag(currnet_sales) over(partition by product_name order by order_year) as previous_year,
case when currnet_sales - lag(currnet_sales) over(partition by product_name order by order_year) > 0 then 'incresing'
	 when currnet_sales - lag(currnet_sales) over(partition by product_name order by order_year) < 0 then 'decresing'
	 else 'no change' end previous_change
from yearly_product_sales
order by product_name, order_year

/* need to focus on products that are below average and decreasing over 2 years
to decide if it needs better product strategy or marketing campaign or stop selling it */

	
with category_sales as (
select 
category,
sum(sales_amount) as total_sales
from gold.fact_sales as f
left join gold.dim_products as p 
on p.product_key = f.product_key
group by category )

select 
category,
sum(total_sales) over() as overall_sales,
concat(round(cast(total_sales as float) / sum(total_sales) over()  *100,2), ' %') as percentage_of_total
from category_sales

-- bikes are 96.64% of total sales

	
with customer_spending as (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_date,
max(order_date) as latest_date,
datediff(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales as f
left join gold.dim_customers as c
on f.customer_key = c.customer_key
group by c.customer_key
)

select 
customer_type,
count(customer_key) as total_customers
from(
	select 
	customer_key,
	total_spending,
	lifespan,
	case when lifespan >= 12 and total_spending > 5000 then 'VIP'
		 when lifespan >= 12 and total_spending < 5000 then 'Regular'
		 else 'New Customer' end customer_type
	from customer_spending) as t
group by customer_type 
order by total_customers desc

/* there is 79.15% of new customers and only 8.96% of VIP customers
I suggest focusing on improving after-sales services to enhance customer loyalty */

