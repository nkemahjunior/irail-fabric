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

# PARAMETERS CELL ********************

layer = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import functions as F


def duplicates(table, key):
    return spark.table(table).groupBy(key).count().filter("count > 1").count()


def nulls(table, cols):
    return spark.table(table).filter(" OR ".join(f"{c} IS NULL" for c in cols)).count()


def orphans(fact, dim, key):
    return spark.table(fact).join(spark.table(dim), key, "left_anti").count()


CHECKS = {
    "silver": [
        ("departures: unique departure_id",  lambda: duplicates("lh_silver.departures", "departure_id")),
        ("departures: keys not null",        lambda: nulls("lh_silver.departures", ["departure_id", "station_id", "scheduled_departure_utc"])),
        ("departures: delay 0 to 12 hours",  lambda: spark.table("lh_silver.departures").filter("delay_seconds < 0 OR delay_seconds > 43200").count()),
        ("disruptions: unique disruption_id", lambda: duplicates("lh_silver.disruptions", "disruption_id")),
        ("stations: unique station_id",      lambda: duplicates("lh_silver.stations", "station_id")),
    ],
    "gold": [
        ("fact: unique departure_id",        lambda: duplicates("lh_gold.fact_departure", "departure_id")),
        ("fact: no orphan date_key",         lambda: orphans("lh_gold.fact_departure", "lh_gold.dim_date", "date_key")),
        ("fact: no orphan time_key",         lambda: orphans("lh_gold.fact_departure", "lh_gold.dim_time", "time_key")),
        ("fact: no orphan station_id",       lambda: orphans("lh_gold.fact_departure", "lh_gold.dim_station", "station_id")),
        ("fact: no orphan train_category",   lambda: orphans("lh_gold.fact_departure", "lh_gold.dim_train_type", "train_category")),
        ("fact: row count matches silver",   lambda: abs(spark.table("lh_gold.fact_departure").count() - spark.table("lh_silver.departures").count())),
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

results_df = (spark.createDataFrame(results, "run_at timestamp, layer string, check_name string, failing_rows long")
              .withColumn("passed", F.col("failing_rows") == 0))
results_df.write.mode("append").format("delta").saveAsTable("lh_gold.dq_results")
display(results_df)

failed = [(name, n) for _, _, name, n in results if n > 0]
if failed:
    raise Exception(f"{len(failed)} data quality check(s) failed: " + "; ".join(f"{name} ({n} rows)" for name, n in failed))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
