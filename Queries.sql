-- change by time analysis - getting the total sales for each date
select order_date,
sum(sales_amount) as total_sales
from `gold.fact_sales`
where order_date is not null
  AND order_date != '' -- add if csv is not fomatted properly to remove null values
group by order_date
order by order_date;

-- getting total sales per year
select year(order_date) as order_year,
sum(sales_amount) as total_sales
from `gold.fact_sales`
where order_date is not null
	and order_date != ''
group by order_year 
order by order_year;

-- total number of customers using customer_key and quantity sold

select year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from `gold.fact_sales`
where order_date is not null
	and order_date != ''
group by order_year 
order by order_year;

-- alternate method to sort using month -
select 
format(order_date, 'yyyy-MM') as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from `gold.fact_sales`
where order_date is not null
	and order_date != ''
group by format(order_date, 'yyyy-MM')
order by format(order_date, 'yyyy-MM');

-- CUMULATIVE Analysis
-- calculate the total sales per month
-- and the running total of sales over time

select
order_date,
total_sales,
-- window function
sum(total_sales) over (order by order_date) as running_total_sales, 
avg(avg_price) over (order by order_date) as moving_avg_price
from
(
select 
format(order_date, 'yyyy-MM') as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from `gold.fact_sales`
where order_date is not null
group by format(order_date, 'yyyy-MM')
) t

-- PERFORMANCE ANALYSIS
-- Analyse the yearly performcae of products by comparing their sales
-- to both the avg sales performace of the product and the previous year's sales

SELECT
  YEAR(f.order_date) AS order_year,
  p.product_name,
  SUM(f.sales_amount) AS current_sales
FROM `gold.fact_sales` f
LEFT JOIN `gold.dim_products` p
  ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY
  YEAR(f.order_date),
  p.product_name;


SELECT
  YEAR(f.order_date) AS order_year,
  p.product_name,
  SUM(f.sales_amount) AS current_sales
FROM `gold.fact_sales` f
LEFT JOIN `gold.dim_products` p
  ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY
  YEAR(f.order_date),
  p.product_name;

-- Part 2 whole analysis
-- which category contribute to the overall sales

select * from `gold.fact_sales`;
select * from `gold.dim_products`; -- has the category column

SELECT
    cs.category,
    cs.total_sales,
    overall.total_sales_all AS overall_sales,
    ROUND((cs.total_sales / overall.total_sales_all) * 100, 2) AS percentage_of_total
FROM (
    SELECT
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM `gold.fact_sales` f
    LEFT JOIN `gold.dim_products` p
        ON p.product_key = f.product_key
    GROUP BY p.category
) cs,
(
    SELECT
        SUM(f.sales_amount) AS total_sales_all
    FROM `gold.fact_sales` f
    LEFT JOIN `gold.dim_products` p
        ON p.product_key = f.product_key
) overall
ORDER BY cs.total_sales DESC;

-- data segmentation 
/*Segment products into cost ranges and 
count how many products fall into each segment*/

SELECT 
    CASE
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'
    END AS cost_range,
    COUNT(product_key) AS total_products
FROM `gold.dim_products`
GROUP BY cost_range
ORDER BY total_products DESC;

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

SELECT 
    customer_segment,
    COUNT(*) AS total_customers
FROM (
    SELECT 
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(f.order_date) AS first_order,
        MAX(f.order_date) AS last_order,
        TIMESTAMPDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) AS lifespan,
        CASE 
            WHEN TIMESTAMPDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) >= 12 
                 AND SUM(f.sales_amount) > 5000 THEN 'VIP'
            WHEN TIMESTAMPDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) >= 12 
                 AND SUM(f.sales_amount) <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM `gold.fact_sales` f
    LEFT JOIN `gold.dim_customers` c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;