# NYC Taxi Data Engineering Pipeline using AWS & Apache Airflow

## Overview

An end-to-end data engineering project that processes NYC taxi datasets using AWS services and Apache Airflow. The pipeline follows a Medallion Architecture (Bronze, Silver, and Gold) to transform raw data into analytics-ready datasets for SQL-based analysis.

## Architecture
<img width="1338" height="598" alt="image" src="https://github.com/user-attachments/assets/54a36bca-b249-4697-b2fc-00d428e495f5" />


## Data Sources

- Yellow Taxi trip data
- Green Taxi trip data
- For-Hire Vehicle (FHV) trip data
- NYC taxi zone lookup data

## Medallion Architecture

### Bronze Layer

- Stores raw source datasets in Amazon S3.
- Preserves the original data for downstream processing.

### Silver Layer

- Uses AWS Glue and PySpark to clean, standardize, and transform raw datasets.
- Stores processed datasets in Amazon S3 in Parquet format.
- Uses AWS Glue Crawlers and the Glue Data Catalog to discover and catalog datasets.

### Gold Layer

A consolidated AWS Glue ETL job creates four analytics-ready fact and dimension datasets:

| Dataset | Description |
|---|---|
| `fact_taxi_trip` | Combined Yellow and Green taxi trip data |
| `fact_fhv_trip` | For-Hire Vehicle trip data |
| `dim_zone` | Taxi zone and borough attributes |
| `dim_date` | Calendar attributes for date-based analysis |

The Gold datasets are stored in Amazon S3 and cataloged using an AWS Glue Crawler.

## Workflow Orchestration

Apache Airflow orchestrates the pipeline tasks, including:

- Data ingestion
- AWS Glue ETL job execution
- Glue Crawler execution
- Task dependencies and execution order

Downstream tasks are configured to run after their required upstream tasks complete successfully.

## Data Analytics with Amazon Athena

Amazon Athena is used to query the cataloged Gold datasets using SQL.

Example business questions:

- Which taxi type generates the highest fare revenue?
- How does trip volume change month by month?
- Which pickup zones have the highest demand?
- What is the average trip distance and duration?
- Which payment methods are most commonly used?
- Are there trips with missing zone matches or suspicious values?

### Sample SQL Query

```sql
SELECT
    source_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(AVG(fare_amount), 2) AS average_fare
FROM nyz_taxi.fact_taxi_trip
GROUP BY source_type
ORDER BY total_fare DESC;
```

*Note: Update the database or column names if they differ in your AWS Glue Data Catalog.*

## Technologies Used

- **Cloud:** AWS
- **Storage:** Amazon S3
- **ETL:** AWS Glue, PySpark
- **Metadata Management:** AWS Glue Data Catalog, Glue Crawlers
- **Query Engine:** Amazon Athena
- **Orchestration:** Apache Airflow
- **Programming:** Python, SQL
- **File Format:** Parquet
- **Version Control:** Git, GitHub

## Future Enhancements

- Build an interactive analytics dashboard using Tableau or Amazon QuickSight.
- Configure dashboard refreshes following successful Gold-layer updates.
- Add automated data quality checks and pipeline failure alerts.
- Implement incremental data processing for newly arriving records.

## Project Status

The S3-based Medallion pipeline, AWS Glue transformations, Glue Crawlers, and Athena analytics are the core project components. Dashboard integration and automated dashboard refresh are planned enhancements.
