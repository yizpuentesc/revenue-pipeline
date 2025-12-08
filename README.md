# Revenue Pipeline --- Gold Layer (Lakehouse)

This project implements a lakehouse architecture based Medallion(Bronze / Silver/ Gold)

The purpose of the pipeline is build product daily revenue for analytical consumption by Marketing and BI tools
(e.g. PowerBI).

-----------------------------------------------------------------------

## 1. Lakehouse Architecture Context

This project assumes the following global platform architecture:

### Bronze

-   Raw data ingestion (events, CDC, files, streams)
-   No transformations
-   Not implemented in this repository (assumed upstream)

### Silver --- Data Vault

-   Integration layer
-   Modeled using Data Vault (Hubs, Links, Satellites)
-   Provides clean, historized, deduplicated data per domain:
    -   Sales
    -   Product
    -   (Future) Pricing
-   Note: This pipeline consumes only silver BV views

### Gold --- Kimball

-   Analytics layer
-   Modeled using Kimball (Facts & Dimensions)
-   This project builds:
    -   Fact: Daily Revenue per Product

------------------------------------------------------------------------

## 2. Business Objective

Marketing requires a dataset that shows:

-   SKU
-   Date
-   Price
-   Units sold
-   Revenue

Including: 
- Days with zero sales
- Incremental updates 
- Reprocessing for late-arriving data 
- Future support for dynamic pricing

------------------------------------------------------------------------

## 3. Gold Fact Definition

**Fact: `revenue_daily`**
``` 
  Column    Description
  --------- -------------------------------
  sku_id    Product identifier/
  date_id   Calendar day/
  price     Unit price (currently static)/
  sales     Units sold per day/
  revenue   price × sales/
``` 
------------------------------------------------------------------------

## 4. Project Structure

``` text
revenue-pipeline/
├─ README.md
├─ requirements.txt
│
├─ conf/
│  └─ revenue_job.yaml
│
├─ src/
│  └─ revenue_pipeline/
│     ├─ __init__.py
│     │
│     ├─ config.py
│     ├─ spark_session.py
│     ├─ logging_utils.py
│     │
│     ├─ sources/                      # How data is read from Silver
│     │   ├─ __init__.py
│     │   ├─ product_reader.py
│     │   └─ sales_reader.py
│     │
│     ├─ schemas/                      # What structure the Gold layer expects
│     │   ├─ __init__.py
│     │   ├─ product_schema.py
│     │   └─ sales_schema.py
│     │
│     ├─ domain/                       # Gold business logic (Kimball)
│     │   ├─ __init__.py
│     │   └─ revenue_transformations.py
│     │
│     ├─ writers/                      # Gold persistence
│     │   ├─ __init__.py
│     │   └─ iceberg_writer.py
│     │
│     └─ dq/                           # Data quality checks
│         ├─ __init__.py
│         └─ checks.py
│
├─ jobs/
│  └─ revenue_iceberg_job.py
│
├─ dags/
│  └─ dag_revenue_iceberg.py
│
└─ tests/
    ├─ __init__.py
    │
    ├─ unit/
    │   ├─ test_product_reader.py
    │   ├─ test_sales_reader.py
    │   ├─ test_revenue_transformations.py
    │   └─ test_iceberg_writer.py
    │
    ├─ integration/
    │   └─ test_revenue_end_to_end.py
    │
    └─ sample_data/
        ├─ product_silver_sample.json
        ├─ sales_silver_sample.json
        └─ expected_revenue_output.json

```
------------------------------------------------------------------------

## 5. Layer Responsibilities (Inside This Project)
```
  Folder       Responsibility
  ------------ ------------------------------------------------------
  `sources/`   Read and normalize data from the silver layer
  `schemas/`   Define the expected structure used by the gold layer
  `domain/`    Apply gold business logic (revenue calculation)
  `writers/`   Persist gold data into iceberg
  `jobs/`      Orchestrate the full pipeline execution
  `dq/`        Data quality validation
  `tests/`     Unit tests
```
------------------------------------------------------------------------


## 6. Incremental Processing & Idempotency

The pipeline supports incremental, idempotent, and reprocessable loads using configurable date windows.  
Iceberg partition overwrites allow safe daily reprocessing, late-arriving corrections, and future CDC recalculations.


------------------------------------------------------------------------

## 7. Partitioning Strategy

Revenue_daily table is partitioned by `date_id` to optimize daily reprocessing and time-based BI access.  
This can evolve to monthly partitioning and SKU clustering without impacting the Kimball model.


------------------------------------------------------------------------

## 8. Execution

Before running the job, the `conf/revenue_job.yaml` file must be configured with the correct silver and gold table names and any
environment-specific connection settings.\
Out of the box, this project provides only the code structure and sample config, not wired production connections.

Local execution:

``` bash
spark-submit jobs/revenue_iceberg_job.py conf/revenue_job.yaml
```

In production, the job is orchestrated by:

-   Airflow (`dags/dag_revenue_iceberg.py`)
-   Or any enterprise scheduler

For safe validation using sample data run the test suite:

``` bash
pytest tests/
```
------------------------------------------------------------------------

## 9. Dynamic Pricing (Future-Ready)

The current model assumes **static price from product**.

The architecture already supports future evolution to:

-   Pricing events
-   Time-valid price ranges
-   CDC-driven revenue recalculation

This will be done by adding: 
    - A new `pricing_reader.py` 
    - A pricing join in `revenue_transformations.py`

------------------------------------------------------------------------

## 10. Testing & Data Quality

-   All business logic is unit-testable via `tests/`
-   Data quality rules live in `dq/`
-   Schema validation is enforced between `sources/` and `domain/`

------------------------------------------------------------------------


## 11. Author

https://github.com/yizpuentesc
