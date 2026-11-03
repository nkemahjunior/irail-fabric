# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c979fb51-8514-4db5-92a9-b5c698e8dff3",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "3d9a52a4-37cb-4643-af42-3d3bee62d970",
# META       "known_lakehouses": [
# META         {
# META           "id": "fa0b35b2-b3c3-4aa8-8659-791f2aee89db"
# META         },
# META         {
# META           "id": "c979fb51-8514-4db5-92a9-b5c698e8dff3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json
from pathlib import Path
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

bronze = notebookutils.lakehouse.getWithProperties("lh_bronze")
BRONZE_LIVEBOARD = bronze["properties"]["abfsPath"] + "/Files/raw/liveboard"

TARGET = "departures"
CHECKPOINT = "Files/_checkpoints/departures"
SCHEMA_FILE = Path("/lakehouse/default/Files/_schemas/liveboard.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window

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




def latest_per_departure(df):
    first_seen = df.groupBy("departure_id").agg(F.min("observed_at").alias("first_seen_at"))
    newest_first = Window.partitionBy("departure_id").orderBy(F.col("observed_at").desc())
    latest = (df.withColumn("rn", F.row_number().over(newest_first))
                .filter(F.col("rn") == 1)
                .drop("rn"))
    return (latest.join(first_seen, "departure_id")
                  .withColumnRenamed("observed_at", "last_seen_at"))



def merge_departures(batch):
    if not spark.catalog.tableExists(TARGET):
        batch.write.format("delta").saveAsTable(TARGET)
        return
    updates = {c: f"s.{c}" for c in batch.columns if c != "first_seen_at"}
    updates["first_seen_at"] = "least(t.first_seen_at, s.first_seen_at)"
    (DeltaTable.forName(spark, TARGET).alias("t")
        .merge(batch.alias("s"), "t.departure_id = s.departure_id")
        .whenMatchedUpdate(condition="s.last_seen_at >= t.last_seen_at", set=updates)
        .whenNotMatchedInsertAll()
        .execute())


def process_micro_batch(new_files_df, batch_id):
    merge_departures(latest_per_departure(flatten_liveboard(new_files_df)))


def liveboard_schema():
    if SCHEMA_FILE.exists():
        return StructType.fromJson(json.loads(SCHEMA_FILE.read_text()))
    schema = spark.read.json(BRONZE_LIVEBOARD).schema
    SCHEMA_FILE.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_FILE.write_text(schema.json())
    return schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = (spark.readStream
            .schema(liveboard_schema())
            .json(BRONZE_LIVEBOARD)
            .withColumn("source_file", F.col("_metadata.file_path"))
         .writeStream
            .foreachBatch(process_micro_batch)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start())
query.awaitTermination()
print("processed batches:", query.recentProgress and len(query.recentProgress))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table = spark.table(TARGET)
print("rows:", table.count(), "| unique departures:", table.select("departure_id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
