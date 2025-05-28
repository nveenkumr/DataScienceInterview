# Databricks notebook source
# MAGIC %md
# MAGIC # Additional PySpark & Spark SQL Interview Questions
# MAGIC Included here are two more advanced problems drawn from Spark scenarios. Each section contains:
# MAGIC 1. **Problem Statement**
# MAGIC 2. **PySpark Solution**
# MAGIC 3. **Spark SQL Solution**
# MAGIC 4. **Sample Output**
# MAGIC 
# MAGIC fileciteturn3file0  

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Find Consecutive Date Ranges per Channel
# MAGIC **Problem:** Given a dataset of `(ChannelId, Date)` entries, identify continuous date ranges for each `ChannelId`—i.e., group consecutive dates into start/end pairs.

# PySpark Solution
display(sdf)  # assumes `sdf` loaded from file above

def find_consecutive_date_ranges(sdf):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    sdf2 = sdf.withColumn("Date", F.to_date("Date"))
    w = Window.partitionBy("ChannelId").orderBy("Date")
    sdf3 = (
        sdf2
        .withColumn("prev_date", F.lag("Date").over(w))
        .withColumn("gap", F.datediff("Date", "prev_date"))
        .withColumn("new_group", F.when(F.col("gap") > 1, 1).otherwise(0))
        .withColumn("grp", F.sum("new_group").over(w))
    )
    result = (
        sdf3
        .groupBy("ChannelId", "grp")
        .agg(
            F.date_format(F.min("Date"), "yyyy-MM-dd").alias("start_date"),
            F.date_format(F.max("Date"), "yyyy-MM-dd").alias("end_date")
        )
        .select("ChannelId", "start_date", "end_date")
    )
    return result

ranges_df = find_consecutive_date_ranges(sdf)
ranges_df.show()
# Sample Output:
# +---------+----------+----------+
# |ChannelId|start_date|  end_date|
# +---------+----------+----------+
# |        A|2024-10-01|2024-10-02|
# |        A|2024-10-04|2024-10-04|
# |        B|2024-10-01|2024-10-01|
# |        B|2024-10-03|2024-10-04|
# |        C|2024-10-01|2024-10-03|
# +---------+----------+----------+

# Spark SQL Solution
sdf.createOrReplaceTempView("channel_dates")
spark.sql("""
WITH with_lag AS (
  SELECT
    ChannelId,
    Date,
    LAG(Date) OVER (PARTITION BY ChannelId ORDER BY Date) AS prev_date
  FROM channel_dates
),
flagged AS (
  SELECT
    ChannelId,
    Date,
    CASE WHEN DATEDIFF(Date, prev_date) > 1 THEN 1 ELSE 0 END AS new_group
  FROM with_lag
),
grouped AS (
  SELECT
    ChannelId,
    Date,
    SUM(new_group) OVER (PARTITION BY ChannelId ORDER BY Date) AS grp_id
  FROM flagged
)
SELECT
  ChannelId,
  DATE_FORMAT(MIN(Date), 'yyyy-MM-dd') AS start_date,
  DATE_FORMAT(MAX(Date), 'yyyy-MM-dd') AS end_date
FROM grouped
GROUP BY ChannelId, grp_id
ORDER BY ChannelId, start_date
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Find Numbers Occurring Three Consecutively
**Problem:** Given a table `emp_numbers(id, numbers)`, find all numbers that occur three times in a row.

# PySpark Solution
```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F
# Assume emp_numbers is a DataFrame
sdf = spark.table("emp_numbers")
w = Window.orderBy("id")
result = (
    sdf
    .withColumn("prev1", F.lag("numbers", 1).over(w))
    .withColumn("prev2", F.lag("numbers", 2).over(w))
    .filter((F.col("numbers") == F.col("prev1")) & (F.col("numbers") == F.col("prev2")))
    .select("id", "numbers")
)
result.show()
# Sample Output:
# +---+-------+
# | id|numbers|
# +---+-------+
# |  3|      2|
# |  8|      4|
# +---+-------+
```

# Spark SQL Solution
```sql
WITH cte AS (
  SELECT 
    id, numbers,
    LAG(numbers,1) OVER (ORDER BY id) AS prev1,
    LAG(numbers,2) OVER (ORDER BY id) AS prev2
  FROM emp_numbers
)
SELECT id, numbers
FROM cte
WHERE numbers = prev1 AND numbers = prev2;
```

## 3. Identify Days with Temperature Increase
**Problem:** Given `emp_temp(Days, Temp)`, find all days where the temperature was higher than the previous day.

# PySpark Solution
```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F
sdf = spark.table("emp_temp").withColumn("Days", F.to_date("Days"))
w = Window.orderBy("Days")
result = (
    sdf
    .withColumn("prev_temp", F.lag("Temp", 1).over(w))
    .filter(F.col("Temp") > F.col("prev_temp"))
)
result.show()
# Sample Output:
# +----------+----+---------+
# |      Days|Temp|prev_temp|
# +----------+----+---------+
# |2024-03-02|  22|       20|
# |2024-03-04|  25|       19|
# +----------+----+---------+
```

# Spark SQL Solution
```sql
SELECT Days, Temp
FROM (
  SELECT *, LAG(Temp) OVER (ORDER BY Days) AS prev_temp
  FROM emp_temp
) t
WHERE Temp > prev_temp;
```

## 4. Fill Missing Dates in a Dataset
**Problem:** Given `emp_gaps(date, id)` where some dates are missing, generate all missing dates between the min and max per partition.

# PySpark Solution
```python
from pyspark.sql import functions as F
# emp_gaps DataFrame with columns date (DateType) and id
# 1. Compute min/max per id
date_range = emp_gaps.groupBy("id").agg(
    F.min("date").alias("start"),
    F.max("date").alias("end")
)
# 2. Generate continuous sequences
dates_seq = date_range.withColumn(
    "full_dates",
    F.expr("sequence(start, end, interval 1 day)")
).select("id", F.explode("full_dates").alias("date"))
# 3. Anti-join to find missing
gaps = (
    dates_seq
    .join(emp_gaps, ["id", "date"], how="anti")
)
gaps.show()
# Sample Output:
# +---+----------+
# | id|      date|
# +---+----------+
# |  1|2022-03-03|
# +---+----------+
```

# Spark SQL Solution
```sql
WITH date_bounds AS (
  SELECT id,
         MIN(date) AS start,
         MAX(date) AS end
  FROM emp_gaps
  GROUP BY id
),
all_dates AS (
  SELECT id,
         EXPLODE(sequence(start, end, interval 1 day)) AS date
  FROM date_bounds
)
SELECT a.id, a.date
FROM all_dates a
LEFT ANTI JOIN emp_gaps g
  ON a.id = g.id AND a.date = g.date;
```
