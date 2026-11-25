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

# MARKDOWN ********************

# # Data quality checks
# Runs the checks for one layer (parameter `layer` = `silver` or `gold`): duplicates, nulls, value ranges, freshness, orphan keys, row counts. Appends results to `lh_gold.dq_results` and fails the pipeline if any check fails.
# **Runs:** `pl_transform`, after each layer

# PARAMETERS CELL ********************

layer = "silver"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import functions as F

# Table names used by the checks
DEPARTURES = "lh_silver.departures"
DISRUPTIONS = "lh_silver.disruptions"
STATIONS = "lh_silver.stations"
FACT = "lh_gold.fact_departure"


def duplicates(table, key):
    return spark.table(table).groupBy(key).count().filter("count > 1").count()


def nulls(table, cols):
    return spark.table(table).filter(" OR ".join(f"{c} IS NULL" for c in cols)).count()


def orphans(fact, dim, key):
    return spark.table(fact).join(spark.table(dim), key, "left_anti").count()


def rows_where(table, condition):
    return spark.table(table).filter(condition).count()


def count_diff(table_a, table_b):
    return abs(spark.table(table_a).count() - spark.table(table_b).count())


def stale(table, column, minutes):
    is_stale = spark.sql(
        f"SELECT max({column}) < current_timestamp() - INTERVAL {minutes} MINUTES FROM {table}"
    ).first()[0]
    return int(is_stale is None or is_stale)


CHECKS = {
    "silver": [
        ("departures: unique departure_id", lambda: duplicates(DEPARTURES, "departure_id")),
        (
            "departures: keys not null",
            lambda: nulls(DEPARTURES, ["departure_id", "station_id", "scheduled_departure_utc"]),
        ),
        (
            "departures: delay 0 to 12 hours",
            lambda: rows_where(DEPARTURES, "delay_seconds < 0 OR delay_seconds > 43200"),
        ),
        ("disruptions: unique disruption_id", lambda: duplicates(DISRUPTIONS, "disruption_id")),
        ("stations: unique station_id", lambda: duplicates(STATIONS, "station_id")),
        ("departures: newest data under 45 min old", lambda: stale(DEPARTURES, "last_seen_at", 45)),
    ],
    "gold": [
        ("fact: unique departure_id", lambda: duplicates(FACT, "departure_id")),
        ("fact: no orphan date_key", lambda: orphans(FACT, "lh_gold.dim_date", "date_key")),
        ("fact: no orphan time_key", lambda: orphans(FACT, "lh_gold.dim_time", "time_key")),
        ("fact: no orphan station_id", lambda: orphans(FACT, "lh_gold.dim_station", "station_id")),
        ("fact: no orphan train_category", lambda: orphans(FACT, "lh_gold.dim_train_type", "train_category")),
        ("fact: row count matches silver", lambda: count_diff(FACT, DEPARTURES)),
    ],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_at = datetime.now(timezone.utc)
results = [(run_at, layer, name, int(check())) for name, check in CHECKS[layer]]

results_df = spark.createDataFrame(
    results, "run_at timestamp, layer string, check_name string, failing_rows long"
).withColumn("passed", F.col("failing_rows") == 0)
results_df.write.mode("append").format("delta").saveAsTable("lh_gold.dq_results")
display(results_df)

failed = [(name, n) for _, _, name, n in results if n > 0]
if failed:
    details = "; ".join(f"{name} ({n} rows)" for name, n in failed)
    raise Exception(f"{len(failed)} data quality check(s) failed: {details}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
