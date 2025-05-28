-- SQL Practice Notebook: Curated Interview Questions
-- This notebook contains de-duplicated SQL problems with schema setup, problem statements, solutions, and sample answers.

-- 1. Numeric Type Casting
-- Problem: Demonstrate FLOAT, DOUBLE, and DECIMAL casts and precision/storage characteristics.
SELECT CAST(1.233334543219210 AS FLOAT)  AS float_value;    -- ~7 digits precision (4 bytes)
-- Answer: float_value ≈ 1.2333345

SELECT CAST(1.233332421340120983721094809 AS DOUBLE) AS double_value; -- ~15-16 digits
-- Answer: double_value ≈ 1.233332421340121

SELECT CAST(1.2333234248012834012834012309481092348 AS DECIMAL(10,4)) AS decimal_10_4;
-- Answer: decimal_10_4 = 1.2333

SELECT CAST(1.2333234248012834012834012309481092348 AS DECIMAL(18,4)) AS decimal_18_4;
-- Answer: decimal_18_4 = 1.2333

SELECT CAST(1.2333234248012834012834012309481092348 AS DECIMAL(38,10)) AS decimal_38_10;
-- Answer: decimal_38_10 = 1.2333234248

-- 2. Get Distinct Products without DISTINCT
-- Problem: Return unique product names from sales table.
CREATE OR REPLACE TEMP VIEW sales(product, quantity) AS VALUES
  ('apple',20),('orange',15),('apple',30),('banana',35);
SELECT product
  FROM sales
 GROUP BY product;
-- Answer:
-- apple
-- orange
-- banana

-- 3. Non-Equi Join
-- Problem: Join table1 and table2 where table1.value > table2.value
CREATE OR REPLACE TEMP VIEW table1(id, value) AS VALUES (1,10),(2,20),(3,30);
CREATE OR REPLACE TEMP VIEW table2(id, value) AS VALUES (1,15),(2,25),(3,35);
SELECT t1.id, t1.value AS t1_val, t2.value AS t2_val
  FROM table1 t1
  JOIN table2 t2
    ON t1.value > t2.value;
-- Answer:
-- (id=2, t1_val=20, t2_val=15)
-- (id=3, t1_val=30, t2_val=10)
-- (id=3, t1_val=30, t2_val=25)

-- 4. Identify Duplicate Rows
-- Problem: Find IDs with more than one occurrence in table1.
SELECT id, COUNT(*) AS cnt
  FROM table1
 GROUP BY id
HAVING COUNT(*) > 1;
-- Answer: None (no id appears twice in table1 view)

-- 5. Third Highest Salary
-- Problem: Find the 3rd highest distinct salary using window functions.
CREATE OR REPLACE TEMP VIEW employees(salary) AS VALUES (3000),(4000),(4000),(5000),(6000);
WITH ranked AS (
  SELECT DISTINCT salary,
         DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
)
SELECT salary
  FROM ranked
 WHERE rnk = 3;
-- Answer: 4000

-- 6. Moving Average of Salaries by Hire Date
-- Problem: Compute a 3-row moving average of salary ordered by hiredate.
CREATE OR REPLACE TEMP VIEW emp(id, salary, hiredate) AS VALUES
  (1,5000,'2024-01-01'),(2,6000,'2024-02-15'),(3,5500,'2024-03-10'),
  (4,6200,'2024-04-05');
SELECT *,
       AVG(salary) OVER (
         ORDER BY hiredate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS moving_avg_3
  FROM emp;
-- Answer:
-- id=1 => moving_avg_3=5000
-- id=2 => moving_avg_3=(5000+6000)/2=5500
-- id=3 => moving_avg_3=(5000+6000+5500)/3≈5500
-- id=4 => moving_avg_3=(6000+5500+6200)/3≈5900

-- 7. Hired Within Last Year
-- Problem: List employees hired within the past 12 months.
SELECT *,
       MONTHS_BETWEEN(CURRENT_DATE(), hiredate) AS months_from_hire
  FROM emp
 WHERE MONTHS_BETWEEN(CURRENT_DATE(), hiredate) <= 12;
-- Answer: All rows if run within a year of latest date

-- 8. Join Types & Null Behavior
-- Problem: Compare INNER, LEFT, RIGHT, and FULL OUTER join counts when NULLs are present.
CREATE OR REPLACE TEMP VIEW t1(id) AS VALUES (10),(10),(NULL);
CREATE OR REPLACE TEMP VIEW t2(id) AS VALUES (10),(NULL);
-- Inner Join
SELECT COUNT(*) AS inner_count FROM t1 JOIN t2 USING(id);
-- Answer: inner_count = 2
-- Left Join
SELECT COUNT(*) AS left_count FROM t1 LEFT JOIN t2 USING(id);
-- Answer: left_count = 4
-- Right Join
SELECT COUNT(*) AS right_count FROM t1 RIGHT JOIN t2 USING(id);
-- Answer: right_count = 3
-- Full Outer Join
SELECT COUNT(*) AS full_count FROM t1 FULL OUTER JOIN t2 USING(id);
-- Answer: full_count = 5

-- 9. Cities with Always Increasing COVID Cases
-- Problem: Find cities where each day's cases > previous day.
CREATE OR REPLACE TEMP VIEW covid(city, report_date, cases) AS VALUES
  ('NY','2023-03-01',100),('NY','2023-03-02',150),('NY','2023-03-03',200),
  ('LA','2023-03-01', 80),('LA','2023-03-02', 85),('LA','2023-03-03', 83),
  ('CHI','2023-03-01',120),('CHI','2023-03-02',120),('CHI','2023-03-03',120);
WITH lagged AS (
  SELECT *, LAG(cases) OVER (PARTITION BY city ORDER BY report_date) AS prev_c
  FROM covid
), flags AS (
  SELECT city, CASE WHEN cases > COALESCE(prev_c,0) THEN 1 ELSE 0 END AS ok
  FROM lagged
)
SELECT city
  FROM flags
 GROUP BY city
HAVING SUM(ok) = COUNT(*);
-- Answer:
-- NY

-- 10. Driver Rides & Profit Rides
-- Problem: For each driver, count total rides and profit rides (end_loc = next start_loc).
CREATE OR REPLACE TEMP VIEW drives(driver, start_time, end_time, start_loc, end_loc) AS VALUES
  ('d1','09:00','09:30','A','B'),('d1','09:30','10:00','B','C'),('d1','11:00','11:30','D','E'),
  ('d2','08:00','08:45','X','Y'),('d2','08:50','09:15','Y','Z');
WITH sequenced AS (
  SELECT *, LEAD(start_loc) OVER (PARTITION BY driver ORDER BY start_time) AS next_start
  FROM drives
)
SELECT driver,
       COUNT(*) AS total_rides,
       SUM(CASE WHEN end_loc = next_start THEN 1 ELSE 0 END) AS profit_rides
  FROM sequenced
 GROUP BY driver;
-- Answer:
-- d1: total_rides=3, profit_rides=1
-- d2: total_rides=2, profit_rides=1

-- 11. Daily Cumulative Balance with Monthly Reset
-- Problem: For each transaction, compute end-of-day balance and reset at month start.
CREATE OR REPLACE TEMP VIEW tx(id, type, amount, ts) AS VALUES
  (1,'deposit',100,'2022-07-01'),(2,'withdrawal',20,'2022-07-01'),
  (3,'deposit',50,'2022-07-31'),(4,'withdrawal',30,'2022-08-01'),
  (5,'deposit',200,'2022-08-15');
WITH signed AS (
  SELECT *, CASE WHEN type='withdrawal' THEN -amount ELSE amount END AS amt
  FROM tx
), daily AS (
  SELECT DATE(ts) AS day, SUM(amt) AS day_sum
    FROM signed GROUP BY DATE(ts)
), cum AS (
  SELECT day,
         SUM(day_sum) OVER (ORDER BY DATE_TRUNC('month',day), day) AS cum_balance
    FROM daily
)
SELECT day, cum_balance
  FROM cum;
-- Answer:
-- 2022-07-01: 80
-- 2022-07-31: 130
-- 2022-08-01: 100
-- 2022-08-15: 300

-- 12. Find Numbers Occurring Three Consecutively
-- Problem: Given emp_numbers(id, numbers), find numbers appearing three times in a row.
CREATE OR REPLACE TEMP VIEW emp_numbers(id, numbers) AS VALUES
  (1,1),(2,1),(3,2),(4,2),(5,2),(6,3),(7,4),(8,4),(9,4);
WITH cte AS (
  SELECT id, numbers,
         LAG(numbers,1) OVER (ORDER BY id) AS prev1,
         LAG(numbers,2) OVER (ORDER BY id) AS prev2
  FROM emp_numbers
)
SELECT id, numbers
  FROM cte
 WHERE numbers = prev1 AND numbers = prev2;
-- Answer:
-- id=5, numbers=2
-- id=9, numbers=4

-- 13. Identify Days with Temperature Increase
-- Problem: From emp_temp(Days, Temp), list days where Temp > previous day.
CREATE OR REPLACE TEMP VIEW emp_temp(Days, Temp) AS VALUES
  ('2024-03-01',20),('2024-03-02',22),('2024-03-03',19),('2024-03-04',25);
WITH lagged AS (
  SELECT Days, Temp,
         LAG(Temp) OVER (ORDER BY Days) AS prev_temp
  FROM emp_temp
)
SELECT Days, Temp
  FROM lagged
 WHERE Temp > prev_temp;
-- Answer:
-- 2024-03-02: 22
-- 2024-03-04: 25

-- 14. Fill Missing Dates in a Dataset
-- Problem: Given emp_gaps(id, date), generate missing dates between min and max per id.
CREATE OR REPLACE TEMP VIEW emp_gaps(id, date) AS VALUES
  (1,'2022-03-01'),(1,'2022-03-02'),(1,'2022-03-04');
WITH date_bounds AS (
  SELECT id,
         MIN(date) AS start_date,
         MAX(date) AS end_date
    FROM emp_gaps
   GROUP BY id
), all_dates AS (
  SELECT id,
         EXPLODE(SEQUENCE(
           TO_DATE(start_date), TO_DATE(end_date), INTERVAL 1 DAY
         )) AS date
    FROM date_bounds
)
SELECT a.id, a.date
  FROM all_dates a
 LEFT ANTI JOIN emp_gaps g
    ON a.id = g.id AND a.date = g.date;
-- Answer:
-- id=1, date=2022-03-03

-- End of SQL practice notebook
