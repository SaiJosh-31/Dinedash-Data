# DineDash Data Pipeline

This repository contains the DineDash source data and the Databricks DLT pipeline.

## Databricks upload layout

Upload the repository data into a Unity Catalog Volume with this layout:

```text
/Volumes/<catalog>/dinedash_bronze/landing/
  dimensions/
  stream_data/
  user_updates.csv
```

GitHub is used for source control only. Auto Loader reads from the Volume after the files are uploaded there.

## DLT configuration

Add `pipeline/dinedash_dlt.py` as the pipeline source and set these pipeline configuration values:

```text
dinedash.orders_path=/Volumes/<catalog>/dinedash_bronze/landing/stream_data
dinedash.dimensions_path=/Volumes/<catalog>/dinedash_bronze/landing/dimensions
dinedash.updates_path=/Volumes/<catalog>/dinedash_bronze/landing/user_updates.csv
```

Set the pipeline target to a Unity Catalog catalog and schema, for example:

```text
Catalog: main
Target schema: dinedash
Storage location: /Volumes/main/dinedash_bronze/pipeline_storage
```

## Missing or corrupt monthly files

The order input uses an Auto Loader directory rather than one hard-coded monthly filename. A missing month therefore produces no rows and does not fail the pipeline. A malformed JSON record is captured in `_rescued_data` and remains available for inspection instead of stopping ingestion.

To monitor missing months, compare the expected month list with `input_file_name()` values in `bronze_orders`. Do not create an empty JSON file as a substitute for a missing month because it can mask the data-quality issue.

## Pipeline outputs

- Bronze: raw orders, dimensions, and customer updates with audit columns
- Silver: typed and validated orders, order items, and dimensions
- Gold: order and transaction facts plus restaurant-day and popular-item KPIs

The source data includes null dimension values, zero prices, duplicate agent IDs, and customer CDC actions. These are intentionally handled in Silver expectations and CDC processing rather than silently altered in Bronze.
