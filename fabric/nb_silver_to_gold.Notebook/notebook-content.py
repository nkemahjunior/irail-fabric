# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e1acee2e-9565-4280-8560-604681fab4c1",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "3d9a52a4-37cb-4643-af42-3d3bee62d970",
# META       "known_lakehouses": [
# META         {
# META           "id": "e1acee2e-9565-4280-8560-604681fab4c1"
# META         },
# META         {
# META           "id": "c979fb51-8514-4db5-92a9-b5c698e8dff3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install holidays --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import holidays
from datetime import date, timedelta
from pyspark.sql import functions as F

START, END = date(2026, 1, 1), date(2027, 12, 31)
be_holidays = holidays.Belgium(years=range(START.year, END.year + 1))

days = [START + timedelta(days=n) for n in range((END - START).days + 1)]
rows = [(d, be_holidays.get(d)) for d in days]

dim_date = (
    spark.createDataFrame(rows, "date date, holiday_name string")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        ((F.dayofweek("date") + 5) % 7 + 1).alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.col("holiday_name").isNotNull().alias("is_public_holiday"),
        "holiday_name",
    )
)
dim_date.write.mode("overwrite").format("delta").saveAsTable("dim_date")
print("dim_date rows:", spark.table("dim_date").count())
display(spark.table("dim_date").filter("is_public_holiday"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hour = F.floor(F.col("minute_of_day") / 60).cast("int")
minute = (F.col("minute_of_day") % 60).cast("int")

dim_time = (
    spark.range(0, 1440).select(F.col("id").cast("int").alias("minute_of_day"))
    .select(
        (hour * 100 + minute).alias("time_key"),
        hour.alias("hour"),
        minute.alias("minute"),
        F.format_string("%02d:%02d", hour, minute).alias("time_label"),
        "minute_of_day",
    )
    .withColumn("day_part",
        F.when(F.col("hour") < 6, "Night")
         .when(F.col("hour") < 9, "Morning peak")
         .when(F.col("hour") < 16, "Daytime")
         .when(F.col("hour") < 19, "Evening peak")
         .otherwise("Evening"))
    .withColumn("is_peak", F.col("day_part").isin("Morning peak", "Evening peak"))
)
dim_time.write.mode("overwrite").format("delta").saveAsTable("dim_time")
print("dim_time rows:", spark.table("dim_time").count())
display(spark.table("dim_time").filter("minute = 0"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
