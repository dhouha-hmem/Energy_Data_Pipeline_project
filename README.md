# Energy Data Pipeline

## Project Overview

This project implements a **local data pipeline** using **PySpark** and **Delta Lake**
to ingest, transform, and analyze electricity **production** and **price** data
from the public API: https://api.energy-charts.info

The pipeline follows the **Medallion Architecture**:

- **Bronze**: raw ingested data (JSON)
- **Silver**: cleaned and structured data
- **Gold**: aggregated, analytics-ready data

## Objectives

- Ingest public power production data for Germany (15-minute resolution)
- Ingest electricity price data for the DE-LU bidding zone
- Support configurable backfilling (historical data ingestion)
- Produce daily aggregated metrics suitable for analysis
- Enable easy configuration without changing code

## Architecture
```
Energy-Charts API
↓
Bronze Layer (raw JSON, Delta)
↓
Silver Layer (parsed & exploded)
↓
Gold Layer (daily aggregates & joins)
```

## Technologies Used

- **Visual Studio Code (VS Code)**
- **Python** 3.11.9  
- **PySpark** 3.5.4  
- **Delta Lake**  
- **YAML** (configuration)  
- **Requests** (API calls)

## Project Structure
```
energy-data-pipeline/
│
├── config/
│ └── config.yaml           #Pipeline configuration 
│
├── src/
│ ├── __init__.py           #Makes the folder a Python package
│ │  
│ ├── ingestion/            #Bronze ingestion logic
│ │ ├── power_ingestion.py
│ │ └── price_ingestion.py
│ │
│ ├── transformations/
│ │ ├── __init__.py 
│ │ ├── bronze_to_silver.py
│ │ └── silver_to_gold.py
│ │
│ └── utils/
│   ├── __init__.py
│   ├── api_client.py       # Energy-Charts API client
│   ├── dates.py            # Date & backfill logic
│   ├── paths.py            # Medallion layer paths
│   └── spark_session.py    # Spark + Delta setup
│
├── data/
│ ├── bronze/
│ ├── silver/
│ └── gold/
│
├── main.py                 # Pipeline entry point
├── requirements.txt
└── README.md
```

The `data/` directory is created locally and used to store pipeline outputs.

## Ingestion Logic

For each date in the configured range:

1. Call Energy-Charts API (`/public_power`, `/price`)
2. Store the raw JSON response in **Bronze** (Delta)
3. Parse and explode arrays into **Silver**
4. Aggregate into daily metrics in **Gold**

## Backfill & Date Handling

Backfilling allows ingesting historical data before daily ingestion starts.

Configured in `config/config.yaml`:

```yaml
run:
  ingestion_frequency: "daily"
  backfill:
    enabled: true
    start_date: "2025-01-01"
    end_date: "2025-01-07"
```
This will ingest 7 days of data, with one API call per day.

All pipeline behavior (date range, backfill window, ingestion frequency) is controlled via config.yaml, ensuring flexibility without code changes.

## Pipeline Setup & Run

### Create and activate a virtual environment
```bash
python -m venv .venv
.venv\Scripts\activate   
```

### Install dependencies
```
pip install -r requirements.txt
```
### Run the pipeline
```
python -m src.main
```
Spark temporary and warehouse directories are created automatically at runtime by the pipeline. No additional manual Spark setup is required.

### Verify results

The pipeline prints row counts for each layer:

- Bronze tables
- Silver tables
- Gold tables

Sample records from Gold tables are displayed to confirm:

- Correct date handling
- Successful aggregations
- Successful joins

## Notes & Design Choices

- Dates are controlled only via config.yaml (no hardcoding)

- Delta Lake is used for all layers to ensure ACID guarantees and reliable local storage

- Aggregations are performed in the Gold layer to keep Silver reusable

- Local execution requires restricted data volume, so backfill ranges are intentionally small
 
- The pipeline is executed as a Python module to ensure
correct package imports.

- The pipeline runs only when executed directly, not when imported


