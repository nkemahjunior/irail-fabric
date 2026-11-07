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

# CELL ********************

tracked = [
    # station_id,          role,       province,          region
    ("BE.NMBS.008813003", "Hub",      "Brussels-Capital", "Brussels"),
    ("BE.NMBS.008814001", "Hub",      "Brussels-Capital", "Brussels"),
    ("BE.NMBS.008812005", "Hub",      "Brussels-Capital", "Brussels"),
    ("BE.NMBS.008821006", "Hub",      "Antwerp",          "Flanders"),
    ("BE.NMBS.008892007", "Hub",      "East Flanders",    "Flanders"),
    ("BE.NMBS.008841004", "Hub",      "Liège",            "Wallonia"),
    ("BE.NMBS.008833001", "Hub",      "Flemish Brabant",  "Flanders"),
    ("BE.NMBS.008891009", "Hub",      "West Flanders",    "Flanders"),
    ("BE.NMBS.008863008", "Hub",      "Namur",            "Wallonia"),
    ("BE.NMBS.008872009", "Hub",      "Hainaut",          "Wallonia"),
    ("BE.NMBS.008831005", "Regional", "Limburg",          "Flanders"),
    ("BE.NMBS.008831765", "Regional", "Limburg",          "Flanders"),
    ("BE.NMBS.008831112", "Regional", "Limburg",          "Flanders"),
]
tracked_df = spark.createDataFrame(tracked, "station_id string, station_role string, province string, region string")

dim_station = (
    spark.table("lh_silver.stations")
    .select("station_id", "station_name", "station_name_local", "latitude", "longitude")
    .join(tracked_df, "station_id", "left")
    .withColumn("is_tracked", F.col("station_role").isNotNull())
    .withColumn("station_role", F.coalesce("station_role", F.lit("Destination only")))
)
dim_station.write.mode("overwrite").format("delta").saveAsTable("dim_station")

print("stations:", spark.table("dim_station").count(),
      "| tracked:", spark.table("dim_station").filter("is_tracked").count())
display(spark.table("dim_station").filter("is_tracked").orderBy("region", "station_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_types = [
    # category, description,                          is_international, is_rail
    ("IC",    "InterCity",                            False, True),
    ("S",     "Suburban (S-train)",                   False, True),
    ("L",     "Local",                                False, True),
    ("P",     "Peak-hour",                            False, True),
    ("EXTRA", "Extra train",                          False, True),
    ("TR",    "Special domestic service",             False, True),
    ("ICE",   "ICE high-speed (Germany)",             True,  True),
    ("EUR",   "Eurostar",                             True,  True),
    ("TGV",   "TGV high-speed (France)",              True,  True),
    ("ES",    "European Sleeper (night)",             True,  True),
    ("NJ",    "Nightjet (night)",                     True,  True),
    ("EC",    "EuroCity",                             True,  True),
    ("ECD",   "EuroCity Direct (Amsterdam)",          True,  True),
    ("OTC",   "Other international (Paris)",          True,  True),
    ("BUS",   "Replacement bus",                      False, False),
]
types_df = spark.createDataFrame(
    train_types, "train_category string, description string, is_international boolean, is_rail boolean")

observed = spark.table("lh_silver.departures").select("train_category").distinct()

dim_train_type = (
    observed.join(types_df, "train_category", "left")
    .withColumn("description", F.coalesce("description", F.lit("Other")))
    .withColumn("is_international", F.coalesce("is_international", F.lit(False)))
    .withColumn("is_rail", F.coalesce("is_rail", F.lit(True)))
)
dim_train_type.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_train_type")

display(spark.table("dim_train_type").orderBy("train_category"))
print("unmapped categories:",
      [r.train_category for r in spark.table("dim_train_type").filter("description = 'Other'").collect()])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(spark.table("lh_silver.departures")
    .filter(F.col("train_category").isin("BUS", "OTC", "TR", "ECD"))
    .groupBy("train_category", "train_line")
    .agg(F.count("*").alias("departures"),
         F.first("train_name").alias("example_train"),
         F.first("station_id").alias("example_from"),
         F.first("destination_name").alias("example_to"))
    .orderBy("train_category")
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

SOURCE, TARGET = "lh_silver.departures", "fact_departure"

if spark.catalog.tableExists(TARGET):
    watermark = spark.table(TARGET).agg(F.max("last_seen_at")).first()[0]
    changes = spark.table(SOURCE).filter(F.col("last_seen_at") >= watermark)
else:
    changes = spark.table(SOURCE)

local_ts = F.from_utc_timestamp("scheduled_departure_utc", "Europe/Brussels")

fact = changes.select(
    "departure_id",
    F.date_format(local_ts, "yyyyMMdd").cast("int").alias("date_key"),
    (F.hour(local_ts) * 100 + F.minute(local_ts)).alias("time_key"),
    "station_id",
    "destination_station_id",
    "train_category",
    "train_line",
    "train_name",
    local_ts.alias("scheduled_departure_local"),
    (local_ts + F.make_interval(secs=F.col("delay_seconds"))).alias("expected_departure_local"),
    "delay_seconds",
    F.round(F.col("delay_seconds") / 60, 1).alias("delay_minutes"),
    ((F.col("delay_seconds") < 60) & ~F.col("is_cancelled")).alias("is_on_time"),
    "is_cancelled",
    "platform_changed",
    "is_extra",
    "occupancy",
    "first_seen_at",
    "last_seen_at",
)

if not spark.catalog.tableExists(TARGET):
    fact.write.format("delta").saveAsTable(TARGET)
else:
    (DeltaTable.forName(spark, TARGET).alias("t")
        .merge(fact.alias("s"), "t.departure_id = s.departure_id")
        .whenMatchedUpdateAll(condition="s.last_seen_at >= t.last_seen_at")
        .whenNotMatchedInsertAll()
        .execute())

print("processed rows:", changes.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_t = spark.table(TARGET)
print("fact rows:", fact_t.count(), "| silver rows:", spark.table(SOURCE).count())

checks = {
    "date_key":       fact_t.join(spark.table("dim_date"), "date_key", "left_anti"),
    "time_key":       fact_t.join(spark.table("dim_time"), "time_key", "left_anti"),
    "station_id":     fact_t.join(spark.table("dim_station"), "station_id", "left_anti"),
    "train_category": fact_t.join(spark.table("dim_train_type"), "train_category", "left_anti"),
}
for key, orphans in checks.items():
    print(f"fact rows with unknown {key}: {orphans.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
