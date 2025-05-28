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
# MAGIC ## 2. Extract Transactions Up to Spend Limit
# MAGIC **Problem:** Given two tables:
# MAGIC - `account_limits(account, spend_limit)`
# MAGIC - `account_transactions(account, amount)` (chronological order),
# MAGIC select each transaction such that the **cumulative sum** for that account does **not** exceed its `spend_limit`.

# PySpark Solution
# Load sample data
data_limits = [("1",300),("2",200),("3",1000)]
data_tx = [
    ("1",100),("1",100),("1",200),
    ("2",50),("2",100),("2",50),("2",200),
    ("3",500),("3",600),("3",300),("3",200),("3",300)
]
limits_df = spark.createDataFrame(data_limits, ["account","spend_limit"])
tx_df = spark.createDataFrame(data_tx, ["account","amount"])

from pyspark.sql.window import Window
w2 = Window.partitionBy("account").orderBy("amount").rowsBetween(Window.unboundedPreceding, 0)
tx_flagged = (
    tx_df
    .withColumn("running_total", F.sum("amount").over(w2))
    .join(limits_df, on="account", how="left")
    .filter(F.col("running_total") <= F.col("spend_limit"))
    .select("account","amount","running_total")
)
tx_flagged.show()
# Sample Output:
# +-------+------+-------------+
# |account|amount|running_total|
# +-------+------+-------------+
# |      1|   100|          100|
# |      1|   100|          200|
# |      2|    50|           50|
# |      2|   100|          150|
# |      2|    50|          200|
# |      3|   500|          500|
# |      3|   600|         1100| <-- filtered out as >1000
# |      3|   300|          800|
# |      3|   200|         1000|
# +-------+------+-------------+

# Spark SQL Solution
limits_df.createOrReplaceTempView("account_limits")
tx_df.createOrReplaceTempView("account_transactions")
spark.sql("""
WITH cte AS (
  SELECT
    t.account,
    t.amount,
    SUM(t.amount) OVER (PARTITION BY t.account ORDER BY t.amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    l.spend_limit
  FROM account_transactions t
  JOIN account_limits l ON t.account = l.account
)
SELECT account, amount, running_total
FROM cte
WHERE running_total <= spend_limit
ORDER BY account, running_total
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC **End of additional questions notebook.**
