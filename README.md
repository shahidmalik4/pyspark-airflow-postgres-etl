# PySpark ETL Pipeline with Apache Airflow

## Overview

This project sets up an ETL pipeline using PySpark and Apache Airflow to extract data from a PostgreSQL database, transform it, and load it into a Railway PostgreSQL cloud database. The PySpark script handles the ETL logic, while Apache Airflow manages the workflow orchestration.

## Project Structure
```
pyspark-airflow-postgres-etl/
├── airflow/
│   └── dags/
│       └── airflow_pyspark_railway.py      # Airflow DAG to trigger PySpark ETL
├── pyspark_files/
│   └── pyspark_airflow_railway.py          # PySpark ETL script
│   └── postgresql-42.7.3.jar               # JDBC driver for PostgreSQL
├── data_files/
│   └── airbnb.csv                          # Sample data file (if used)
├── requirements.txt                        # Python dependencies for the project
├── README.md                               # Project overview and setup instructions
└── .gitignore                              # Git ignore file to exclude unnecessary files
```
## Tech Stack

- **Apache Airflow**: Workflow orchestration and scheduling.
- **PySpark**: Data processing and transformations.
- **PostgreSQL**: Source and target database for the ETL process.
- **Railway PostgreSQL**: Cloud-hosted PostgreSQL database for storing transformed data.
- **JDBC**: PostgreSQL JDBC driver for database connection.

## Prerequisites

- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) installed
- [PySpark](https://spark.apache.org/docs/latest/api/python/) installed
- PostgreSQL database (local or cloud)
- Railway PostgreSQL cloud database
- Java Runtime Environment (JRE) for PostgreSQL JDBC driver
- Python 3.x

## Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/shahidmalik4/pyspark-airflow-postgres-etl.git
   cd pyspark-airflow-postgres-etl
   ```
2. **Install Python Dependencies:**
   
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure PostgreSQL Connection:**

   - Update the jdbc_url and properties in pyspark_airflow_railway.py with your local PostgreSQL connection details.
   - Update the railway_jdbc_url and railway_properties with your Railway PostgreSQL connection details.

4. **Setup Airflow:**

   - Initialize Airflow metadata database:

   ```bash
   airflow db init
   ```
6. **Run the Project:**

   ```bash
   airflow webserver -p 8080
   airflow scheduler
   ```

7. **Trigger the DAG:**
   
   - Navigate to http://localhost:8080 to access the Airflow web interface.
   - Trigger the DAG to execute the ETL process.
