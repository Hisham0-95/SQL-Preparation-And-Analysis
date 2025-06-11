/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\hisham\Documents\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\hisham\Documents\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\hisham\Documents\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO


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

-- 2013 was signifcantly higher sales but it didnt last long as it dropped in jan 2014

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
group by year(f.order_date), p.product_name 

)

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

-- bikes has almost all the sales


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

-- /1/ base query: retrives core coulmns from tables

with base_query as(
select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ' , c.last_name) as customer_name,
datediff(year, c.birthdate, GETDATE()) as customer_age
from gold.fact_sales as f 
left join gold.dim_customers as c on c.customer_key = f.customer_key
where order_date is not null )

 select * from base_query  -- works fine

-- /2/ valuable KPIs 


with base_query_for_KPIs as(
select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ' , c.last_name) as customer_name,
datediff(year, c.birthdate, GETDATE()) as customer_age
from gold.fact_sales as f 
left join gold.dim_customers as c on c.customer_key = f.customer_key
where order_date is not null )

select
customer_key,
customer_name,
customer_age,
count(distinct order_number) as total_orders,
sum(quantity) as total_Quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_purchased,
datediff(month, min(order_date), max(order_date)) as lifespan
from base_query_for_KPIs
group by 
customer_key,
customer_name,
customer_age