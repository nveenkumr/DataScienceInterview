# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark & Spark SQL Interview Questions
# MAGIC This notebook contains a comprehensive set of PySpark and Spark SQL interview problems. For each problem:
# MAGIC 1. **Problem Statement**
# MAGIC 2. **PySpark Solution**
# MAGIC 3. **Spark SQL Solution**
# MAGIC 4. **Sample Output**

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Compute Final Amount per Account
# MAGIC **Problem:** Given a `transactions(id, type, amount)` table, calculate for each `id` the final balance, treating `debit` as negative and `credit` as positive.

# PySpark Solution
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
tx = [(1, "credit", 30.0), (1, "debit", 90.0), (2, "credit", 50.0), (3, "debit", 57.0), (2, "debit", 90.0)]
t_df = spark.createDataFrame(tx, ["id","type","amount"] )
res1 = (t_df
    .withColumn("signed_amount", F.when(F.col("type")=="debit", -F.col("amount")).otherwise(F.col("amount")))
    .groupBy("id").agg(F.sum("signed_amount").alias("final_amount")))
res1.show()
# Sample Output:
# +---+------------+
# | id|final_amount|
# +---+------------+
# |  1|       -60.0|
# |  2|       -40.0|
# |  3|       -57.0|
# +---+------------+

# Spark SQL Solution
t_df.createOrReplaceTempView("transactions")
spark.sql("""
 SELECT id,
        SUM(CASE WHEN type='debit' THEN -amount ELSE amount END) AS final_amount
   FROM transactions
  GROUP BY id
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Identify Banned Accounts on Concurrent Logins
# MAGIC **Problem:** Given `logins(account_id, ip, log_in, log_out)`, find accounts with overlapping sessions (log_out >= next log_in).

# PySpark Solution
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead, to_timestamp
sessions = [
    (1,'1.1.1.1','2021-02-01 08:00:00','2021-02-01 11:30:00'),
    (1,'2.2.2.2','2021-02-01 09:00:00','2021-02-01 09:30:00'),
    (2,'3.3.3.3','2021-02-01 20:30:00','2021-02-01 22:00:00'),
    (3,'4.4.4.4','2021-02-01 16:00:00','2021-02-01 16:59:59'),
    (3,'5.5.5.5','2021-02-01 17:00:00','2021-02-01 17:59:59')
]
s_df = spark.createDataFrame(sessions, ['account_id','ip','log_in','log_out'])
s_df = s_df.withColumn('log_in', to_timestamp('log_in')).withColumn('log_out', to_timestamp('log_out'))
w = Window.partitionBy('account_id').orderBy('log_in')
banned = (s_df
    .withColumn('next_login', lead('log_in').over(w))
    .filter(col('log_out') >= col('next_login'))
    .select('account_id').distinct())
banned.show()
# Sample Output: account_id = 1

# Spark SQL Solution
s_df.createOrReplaceTempView('logins')
spark.sql("""
 WITH cte AS (
   SELECT *, LEAD(log_in) OVER (PARTITION BY account_id ORDER BY log_in) AS next_login
   FROM logins
 )
 SELECT DISTINCT account_id
   FROM cte
  WHERE log_out >= next_login
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Cumulative Salary Over Records
# MAGIC **Problem:** Given `(id, name, salary)`, compute running total per employee and overall.

# PySpark Solution
from pyspark.sql.window import Window
data = [(1,'A',1000),(1,'A',1001),(2,'B',2000),(2,'B',2001),(2,'B',2002)]
df = spark.createDataFrame(data, ['id','name','salary'])
w1 = Window.partitionBy('name').orderBy('salary').rowsBetween(Window.unboundedPreceding,0)
df.withColumn('cum_by_name', F.sum('salary').over(w1)).show()
w2 = Window.orderBy('id').rowsBetween(Window.unboundedPreceding,0)
df.withColumn('cum_overall', F.sum('salary').over(w2)).show()

# Spark SQL Solution
df.createOrReplaceTempView('salaries')
spark.sql("""
 SELECT *,
        SUM(salary) OVER (PARTITION BY name ORDER BY salary) AS cum_by_name,
        SUM(salary) OVER (ORDER BY id) AS cum_overall
   FROM salaries
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Department Min/Max Salary
# MAGIC **Problem:** For `(emp_name, dept_id, salary)`, find min and max salary per department.

# PySpark Solution
emp = [('Jaimin',2,80000),('Pankaj',2,80000),('Tarvares',2,70000),('Marlania',4,70000)]
df_emp = spark.createDataFrame(emp, ['emp_name','dept_id','salary'])
df_emp.groupBy('dept_id').agg(
    F.min('salary').alias('min_salary'),
    F.max('salary').alias('max_salary')
).show()

# Spark SQL Solution
df_emp.createOrReplaceTempView('emp_dept')
spark.sql("""
 SELECT dept_id, MIN(salary) AS min_salary, MAX(salary) AS max_salary
   FROM emp_dept GROUP BY dept_id
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Moving Average Over Time Window
# MAGIC **Problem:** Given `(day, product, price)`, compute a 6-day moving average.

# PySpark Solution
data = [(1,'A',100),(2,'A',110),(3,'A',105),(4,'A',115),(5,'A',120),(6,'A',125)]
df_price = spark.createDataFrame(data,['day','product','price'])
w = Window.partitionBy('product').orderBy('day').rowsBetween(-5,0)
df_price.withColumn('mv_avg_6', F.avg('price').over(w)).show()

# Spark SQL Solution
df_price.createOrReplaceTempView('prices')
spark.sql("""
 SELECT *, AVG(price) OVER (
   PARTITION BY product ORDER BY day ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
 ) AS mv_avg_6
 FROM prices
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Dense Rank of Salaries
# MAGIC **Problem:** Assign a dense rank to each employee based on salary descending.

# PySpark Solution
from pyspark.sql.functions import dense_rank
w3 = Window.orderBy(F.col('salary').desc())
df.withColumn('dense_rank', dense_rank().over(w3)).show()

# Spark SQL Solution
df.createOrReplaceTempView('emp_sal')
spark.sql("""
 SELECT *, DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank
   FROM emp_sal
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Explode Arrays
# MAGIC **Problem:** Given `users(id, tags:Array<String>)`, output one row per user-tag pair.

# PySpark Solution
users = [(1,['a','b']), (2,['x']), (3,['y','z','w'])]
df_users = spark.createDataFrame(users,['id','tags'])
df_users.select('id', F.explode('tags').alias('tag')).show()

# Spark SQL Solution
df_users.createOrReplaceTempView('users')
spark.sql("""
 SELECT id, EXPLODE(tags) AS tag FROM users
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Null Cleaning
# MAGIC **Problem:** Replace nulls in numeric columns with zero and blanks in strings with 'NA'.

# PySpark Solution
df_clean = df_users.fillna({'id':0}).na.fill('NA', subset=['tags'])
df_clean.show()

# Spark SQL Solution
# Create view with some nulls for demo
spark.sql("""
 SELECT NULL AS id, NULL AS tags
""").createOrReplaceTempView('t_null')
spark.sql("""
 SELECT COALESCE(id,0) AS id,
        COALESCE(tags,'NA') AS tags
   FROM t_null
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Sessionization
# MAGIC **Problem:** Given `(user, ts)`, assign session IDs where gap > 30 mins starts new session.

# PySpark Solution
from pyspark.sql.functions import lag, unix_timestamp
from pyspark.sql.window import Window
events = [(1,'2021-01-01 10:00:00'),(1,'2021-01-01 10:20:00'),(1,'2021-01-01 11:00:00')]
df_evt = spark.createDataFrame(events,['user','ts'])
df_evt = df_evt.withColumn('ts', to_timestamp('ts'))
w4 = Window.partitionBy('user').orderBy('ts')
df_sess = (df_evt
  .withColumn('prev_ts', lag('ts').over(w4))
  .withColumn('gap', unix_timestamp('ts')-unix_timestamp('prev_ts'))
  .withColumn('session', F.sum(F.when(F.col('gap')>1800,1).otherwise(0)).over(w4))
)
df_sess.show()

# Spark SQL Solution
# similar logic using LAG and SUM OVER

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Missing Dates
# MAGIC **Problem:** Given a date column, add missing dates between min and max for each partition.

# PySpark Solution
from pyspark.sql.functions import sequence, explode, min as min_, max as max_
dates = [(1,'2021-01-01'),(1,'2021-01-04')]
df_dates = spark.createDataFrame(dates,['id','day']).withColumn('day', to_timestamp('day'))
res = (df_dates
  .groupBy('id')
  .agg(sequence(min_('day'), max_('day'), F.expr('interval 1 day')).alias('all_days'))
  .select('id', explode('all_days').alias('day'))
)
res.show()

# Spark SQL Solution
# Use GENERATE_SERIES (in Spark 3.4+) or custom UDF to expand dates

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. Pivot Table
# MAGIC **Problem:** Given `(user, action, count)`, pivot so actions become columns.

# PySpark Solution
acts = [(1,'view',10),(1,'click',4),(2,'view',3)]
df_act = spark.createDataFrame(acts,['user','action','cnt'])
df_act.groupBy('user').pivot('action').sum('cnt').fillna(0).show()

# Spark SQL Solution
df_act.createOrReplaceTempView('actions')
spark.sql("""
 SELECT * FROM (
   SELECT user, action, cnt FROM actions
 )
 PIVOT (SUM(cnt) FOR action IN ('view','click'))
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 12. Device Counts by OS
# MAGIC **Problem:** Count distinct devices per OS from `(device_id, os_type)`.

# PySpark Solution
dev = [(1,'iOS'),(2,'Android'),(3,'iOS'),(2,'Android')]
df_dev = spark.createDataFrame(dev,['device','os'])
df_dev.groupBy('os').agg(F.countDistinct('device').alias('count')).show()

# Spark SQL Solution
df_dev.createOrReplaceTempView('devices')
spark.sql("""
 SELECT os, COUNT(DISTINCT device) AS count FROM devices GROUP BY os
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 13. Third Highest Salary in SQL
# MAGIC **Problem:** Find the 3rd highest salary from `employees(salary)` without using `LIMIT`.

# PySpark Solution
# Use Spark SQL on DataFrame view
spark.sql("""
 SELECT DISTINCT salary FROM employees
 ORDER BY salary DESC
 OFFSET 2 ROWS FETCH NEXT 1 ROWS ONLY
""").show()

# Spark SQL Solution
spark.sql("""
 SELECT salary FROM (
   SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
   FROM employees
 ) tmp
 WHERE rnk = 3
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC **End of comprehensive interview notebook.**
