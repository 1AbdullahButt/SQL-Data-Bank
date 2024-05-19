select * from customer_nodes;
select * from region;
select * from customer_transactions;

SET sql_safe_updates = 0;

alter table customer_nodes
modify column start_date date,
modify column end_date date;

UPDATE customer_nodes
SET start_date = STR_TO_DATE(start_date, '%Y-%m-%d'),
    end_date = STR_TO_DATE(end_date, '%Y-%m-%d');
    
alter table customer_transactions
modify column txn_date date;

UPDATE customer_transactions
SET txn_date  = STR_TO_DATE(txn_date , '%Y-%m-%d');
    

-- A. Customer Nodes Exploration

-- 1. How many unique nodes are there on the Data Bank system?

select count(distinct node_id) as Unique_nodes
from customer_nodes
order by node_id;


-- 2. What is the number of nodes per region?

select r.region_name, count(cn.node_id) as Number_of_Nodes
from customer_nodes cn
join region r
	on cn.region_id = r.region_id
group by region_name
order by Number_of_Nodes;

-- 3. How many customers are allocated to each region?

select r.region_name, r.region_id, count(distinct cn.customer_id) as Number_of_Customers
from customer_nodes cn
join region r
	on cn.region_id = r.region_id
group by region_name, r.region_id
order by Number_of_Customers;

-- 4. How many days on average are customers reallocated to a different node?

select round(avg(datediff(end_date, start_date)),2) as Average_reallocated_days
from customer_nodes
where end_date is Not Null and end_date Not like '9999%';

-- 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

WITH rows_ AS (
    SELECT 
        cn.customer_id,
        r.region_name,
        DATEDIFF(cn.end_date, cn.start_date) AS days_difference,
        ROW_NUMBER() OVER (PARTITION BY r.region_name ORDER BY DATEDIFF(cn.end_date, cn.start_date)) AS rows_number,
        COUNT(*) OVER (PARTITION BY r.region_name) AS total_rows
    FROM 
        customer_nodes cn
    JOIN 
        region r ON cn.region_id = r.region_id
    WHERE 
        cn.end_date IS NOT NULL AND cn.end_date NOT LIKE '9999%'
)
SELECT 
    region_name,
    AVG(days_difference) AS Average_reallocated_days,
    MAX(CASE WHEN rows_number = total_rows / 2 THEN days_difference END) AS Median,
    MAX(CASE WHEN rows_number = ROUND(0.8 * total_rows) THEN days_difference END) AS Percentile_80th,
    MAX(CASE WHEN rows_number = ROUND(0.95 * total_rows) THEN days_difference END) AS Percentile_95th
FROM 
    rows_
GROUP BY 
    region_name;
    
    
-- B. Customer Transactions

-- 1. What is the unique count and total amount for each transaction type?

select txn_type, count(distinct customer_id) Unique_Count, sum(txn_amount) Total_Amount
from customer_transactions
group by txn_type;

-- 2. What is the average total historical deposit counts and amounts for all customers?

select avg(Deposits_Count), avg(Total_Amount)
from (
		select customer_id, count(distinct txn_amount) Deposits_Count, sum(txn_amount) Total_Amount
        from customer_transactions
        where txn_type = 'deposit'
        group by customer_id) as Sub_query;
        
-- 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?

With DB_Customers AS(
select customer_id, month(txn_date) as Month_Count, monthname(txn_date) as Month_Name,
count(case when txn_type = 'deposit' then 1 end) Deposit_Count,
count(case when txn_type = 'purchase' then 1 end) Purchase_Count,
count(case when txn_type = 'withdrawal' then 1 end) Withdrawal_Count
from customer_transactions
group by customer_id, Month_Count, Month_Name)
select Month_Count, Month_Name, count(customer_id)
from DB_Customers
where Deposit_Count > 1 and (Purchase_Count > 0 or Withdrawal_Count > 0)
group by Month_Count, Month_Name;


-- 4. What is the closing balance for each customer at the end of the month?

SELECT
	customer_id,
	MONTH(txn_date) AS month, MONTHNAME(txn_date) AS month_name,
	SUM(
		SUM(CASE WHEN txn_type='deposit' THEN txn_amount ELSE -txn_amount END)) 
		OVER (PARTITION BY customer_id ORDER BY MONTH(txn_date) ROWS UNBOUNDED PRECEDING) 
        AS closing_balance
FROM customer_transactions
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;


-- 5. What is the percentage of customers who increase their closing balance by more than 5%?

SELECT 
    ROUND(
        SUM((bal - prevbal)/prevbal > 0.05)/COUNT(DISTINCT customer_id)*100, 2) 
        as pct_customers
FROM (
	SELECT
	    *, 
	    LAG(bal) OVER (PARTITION BY customer_id ORDER BY month) as prevbal
	FROM (
	    SELECT
			customer_id, 
			MONTH(txn_date) AS month,
			SUM(
			    SUM(
			        CASE WHEN txn_type='deposit' 
			        THEN txn_amount ELSE -txn_amount END)) 
		            OVER (PARTITION BY customer_id ORDER BY MONTH(txn_date) 
                    ROWS UNBOUNDED PRECEDING) AS bal,
		    ROW_NUMBER() OVER (PARTITION BY customer_id 
		        ORDER BY MONTH(txn_date) DESC) AS row_num
        FROM customer_transactions 
        GROUP BY 1, 2 
        ORDER BY 1, 2
        ) AS m
    ) AS f
WHERE row_num = 1;


-- ● Option 1: data is allocated based off the amount of money at the end of the previous month

SELECT customer_id,
       txn_date,
       txn_type,
       txn_amount,
       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
		WHEN txn_type = 'withdrawal' THEN -txn_amount
		WHEN txn_type = 'purchase' THEN -txn_amount
		ELSE 0
	   END) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions;


-- ● Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days

SELECT 
    customer_id,
    MONTH(txn_date) AS month,
    MONTHNAME(txn_date) AS month_name,
    SUM(
        CASE 
            WHEN txn_type = 'deposit' THEN txn_amount
            WHEN txn_type IN ('withdrawal', 'purchase') THEN -txn_amount
            ELSE 0
        END
    ) AS closing_balance
FROM 
    customer_transactions
GROUP BY 
    customer_id, MONTH(txn_date), MONTHNAME(txn_date);


-- Option 3: data is updated real-time
-- For this multi-part challenge question - you have been requested to generate
-- the following data elements to help the Data Bank team estimate how much
-- data will need to be provisioned for each option:
-- ● running customer balance column that includes the impact each
-- transaction
-- ● customer balance at the end of each month
-- ● minimum, average and maximum values of the running balance for each
-- customer
-- Using all of the data available - how much data would have been required for
-- each option on a monthly basis?

WITH running_balance AS
(
	SELECT customer_id,
	       txn_date,
	       txn_type,
	       txn_amount,
	       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			WHEN txn_type = 'withdrawal' THEN -txn_amount
			WHEN txn_type = 'purchase' THEN -txn_amount
			ELSE 0
		   END) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
	FROM customer_transactions
)

SELECT customer_id,
       AVG(running_balance) AS avg_running_balance,
       MIN(running_balance) AS min_running_balance,
       MAX(running_balance) AS max_running_balance
FROM running_balance
GROUP BY customer_id;


-- D. Extra Challenge

-- Data Bank wants to try another option which is a bit more difficult to
-- implement - they want to calculate data growth using an interest calculation,
-- just like in a traditional savings account you might have with a bank.
-- If the annual interest rate is set at 6% and the Data Bank team wants to reward
-- its customers by increasing their data allocation based off the interest
-- calculated on a daily basis at the end of each day, how much data would be
-- required for this option on a monthly basis?
-- Special notes:
-- ● Data Bank wants an initial calculation which does not allow for
-- compounding interest, however they may also be interested in a daily
-- compounding interest calculation so you can try to perform this
-- calculation if you have the stamina!

WITH adjusted_amount AS (
SELECT customer_id, 
EXTRACT(MONTH FROM(txn_date)) AS month_number,
MONTHNAME(txn_date) AS month,
SUM(CASE 
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END) AS monthly_amount
FROM customer_transactions
GROUP BY 1,2,3
ORDER BY 1
),
interest AS (
SELECT customer_id, month_number,month, monthly_amount,
ROUND(((monthly_amount * 6 * 1)/(100 * 12)),2) AS interest
FROM adjusted_amount
GROUP BY 1,2,3,4
ORDER BY 1,2,3
),
total_earnings AS (
SELECT customer_id, month_number, month,
(monthly_amount + interest) as earnings
FROM  interest
GROUP BY 1,2,3,4
ORDER BY 1,2,3
)
SELECT month_number,month,
SUM(CASE WHEN earnings < 0 THEN 0 ELSE earnings END) AS allocation
FROM total_earnings
GROUP BY 1,2
ORDER BY 1,2;