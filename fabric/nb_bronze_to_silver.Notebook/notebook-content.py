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
# META           "id": "fa0b35b2-b3c3-4aa8-8659-791f2aee89db"
# META         },
# META         {
# META           "id": "c979fb51-8514-4db5-92a9-b5c698e8dff3"
# META         },
# META         {
# META           "id": "e1acee2e-9565-4280-8560-604681fab4c1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

def files_path(lakehouse):
    return notebookutils.lakehouse.getWithProperties(lakehouse)["properties"]["abfsPath"] + "/Files"

BRONZE_RAW = files_path("lh_bronze") + "/raw"
SILVER_FILES = files_path("lh_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def keep_latest(df, key):
    first_seen = df.groupBy(key).agg(F.min("observed_at").alias("first_seen_at"))
    newest_first = Window.partitionBy(key).orderBy(F.col("observed_at").desc())
    latest = (df.withColumn("rn", F.row_number().over(newest_first))
                .filter(F.col("rn") == 1).drop("rn"))
    return latest.join(first_seen, key).withColumnRenamed("observed_at", "last_seen_at")


def merge_latest(batch, target, key):
    if not spark.catalog.tableExists(target):
        batch.write.format("delta").saveAsTable(target)
        return
    updates = {c: f"s.{c}" for c in batch.columns if c != "first_seen_at"}
    updates["first_seen_at"] = "least(t.first_seen_at, s.first_seen_at)"
    (DeltaTable.forName(spark, target).alias("t")
        .merge(batch.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdate(condition="s.last_seen_at >= t.last_seen_at", set=updates)
        .whenNotMatchedInsertAll()
        .execute())


def schema_for(endpoint):
    path = f"{SILVER_FILES}/_schemas/{endpoint}.json"
    if notebookutils.fs.exists(path):
        return StructType.fromJson(json.loads(notebookutils.fs.head(path, 1_000_000)))
    schema = spark.read.json(f"{BRONZE_RAW}/{endpoint}").schema
    notebookutils.fs.put(path, schema.json(), True)
    return schema


def run_incremental(endpoint, flatten, table, key):
    target = f"lh_silver.{table}"

    def process(new_files_df, batch_id):
        merge_latest(keep_latest(flatten(new_files_df), key), target, key)

    query = (spark.readStream
                .schema(schema_for(endpoint))
                .json(f"{BRONZE_RAW}/{endpoint}")
                .withColumn("source_file", F.col("_metadata.file_path"))
             .writeStream
                .foreachBatch(process)
                .option("checkpointLocation", f"{SILVER_FILES}/_checkpoints/{table}")
                .trigger(availableNow=True)
                .start())
    query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_liveboard(raw):
    rows = raw.select(
        F.col("stationinfo.id").alias("station_id"),
        F.timestamp_seconds(F.col("timestamp").cast("long")).alias("observed_at"),
        F.col("source_file"),
        F.explode("departures.departure").alias("d"),
    )
    return rows.select(
        F.col("d.departureConnection").alias("departure_id"),
        "station_id",
        F.to_date(F.regexp_extract("d.departureConnection", r"/(\d{8})/", 1), "yyyyMMdd").alias("service_date"),
        F.col("d.vehicle").alias("train_id"),
        F.col("d.vehicleinfo.shortname").alias("train_name"),
        F.col("d.vehicleinfo.type").alias("train_line"),
        F.regexp_extract("d.vehicleinfo.type", r"^([A-Z]+)", 1).alias("train_category"),
        F.col("d.stationinfo.id").alias("destination_station_id"),
        F.col("d.station").alias("destination_name"),
        F.timestamp_seconds(F.col("d.time").cast("long")).alias("scheduled_departure_utc"),
        F.col("d.delay").cast("int").alias("delay_seconds"),
        (F.col("d.canceled") == "1").alias("is_cancelled"),
        (F.col("d.left") == "1").alias("has_left"),
        (F.col("d.isExtra") == "1").alias("is_extra"),
        F.col("d.platform").alias("platform"),
        (F.col("d.platforminfo.normal") == "0").alias("platform_changed"),
        F.col("d.occupancy.name").alias("occupancy"),
        "observed_at",
        "source_file",
    )


def flatten_disturbances(raw):
    rows = raw.select(
        F.timestamp_seconds(F.col("timestamp").cast("long")).alias("observed_at"),
        F.col("source_file"),
        F.explode("disturbance").alias("x"),
    )
    message_id = F.regexp_extract("x.link", r"messageID=(\d+)", 1)
    cause = F.trim(F.regexp_extract("x.description", r"Cause:\s*([^\n]+)", 1))
    return rows.select(
        F.when(message_id != "", message_id)
         .otherwise(F.sha2(F.concat_ws("|", "x.type", "x.title"), 256)).alias("disruption_id"),
        F.col("x.type").alias("disruption_type"),
        F.col("x.title").alias("title"),
        F.trim(F.split("x.title", ":").getItem(0)).alias("affected_section"),
        F.trim(F.regexp_extract("x.title", r":\s*(.+)$", 1)).alias("effect"),
        F.when(cause != "", cause).alias("cause"),
        F.col("x.description").alias("description"),
        F.timestamp_seconds(F.col("x.timestamp").cast("long")).alias("published_at"),
        F.col("x.link").alias("link"),
        "observed_at",
        "source_file",
    )


def flatten_stations(raw):
    rows = raw.select(
        F.timestamp_seconds(F.col("timestamp").cast("long")).alias("observed_at"),
        F.col("source_file"),
        F.explode("station").alias("s"),
    )
    return rows.select(
        F.col("s.id").alias("station_id"),
        F.col("s.name").alias("station_name"),
        F.col("s.standardname").alias("station_name_local"),
        F.col("s.locationY").cast("double").alias("latitude"),
        F.col("s.locationX").cast("double").alias("longitude"),
        "observed_at",
        "source_file",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_incremental("liveboard", flatten_liveboard, "departures", "departure_id")
run_incremental("disturbances", flatten_disturbances, "disruptions", "disruption_id")
run_incremental("stations", flatten_stations, "stations", "station_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table, key in [("departures", "departure_id"), ("disruptions", "disruption_id"), ("stations", "station_id")]:
    t = spark.table(f"lh_silver.{table}")
    print(f"{table}: rows {t.count()} | unique {t.select(key).distinct().count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
