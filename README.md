# PySpark ETL Pipeline with Apache Airflow

## Overview

This project sets up an ETL pipeline using PySpark and Apache Airflow to extract data from a PostgreSQL database, transform it, and load it into a Railway PostgreSQL cloud database. The PySpark script handles the ETL logic, while Apache Airflow manages the workflow orchestration.

## Project Structure

pyspark-airflow-postgres-etl/
├── airflow/
│   └── dags/
│       └── airflow_script.py            # Airflow DAG to trigger PySpark ETL
├── pyspark/
│   └── pyspark_script.py                # PySpark ETL script
├── data_file/
│   └── source_data.csv                  # Sample data file (if used)
├── Dockerfile                           # Dockerfile for containerizing the project (optional)
├── docker-compose.yml                   # Docker Compose file (optional)
├── requirements.txt                     # Python dependencies for the project
├── README.md                            # Project overview and setup instructions
└── .gitignore                           # Git ignore file to exclude unnecessary files


## Prerequisites

- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) installed
- [PySpark](https://spark.apache.org/docs/latest/api/python/) installed
- PostgreSQL database (local or cloud)
- Railway PostgreSQL cloud database
- Java Runtime Environment (JRE) for PostgreSQL JDBC driver

## Setup

1. **Configure PostgreSQL Connection:**
   - Update the `jdbc_url` and `properties` in `pyspark_script.py` with your local PostgreSQL connection details.
   - Update the `railway_jdbc_url` and `railway_properties` with your Railway PostgreSQL connection details.

2. **PySpark Script:**
   - Path: `/home/cipher/pyspark/pyspark_script.py`
   - This script reads data from PostgreSQL, performs transformations, and writes cleaned data to Railway PostgreSQL.

3. **Airflow DAG Script:**
   - Path: `/home/cipher/airflow/dags/airflow_script.py`
   - This DAG triggers the PySpark ETL script manually using `SparkSubmitOperator`.

4. **Airflow Setup:**
   - Ensure Airflow is configured to connect to your Spark cluster.
   - Set the `conn_id` in `airflow_script.py` to match your Airflow Spark connection configuration.

## Running the Project

1. **Start Airflow:**
   ```bash
   airflow webserver -p 8080
   airflow scheduler
