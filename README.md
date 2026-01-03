# 📘 Airflow Data Workflows Project

This project demonstrates an end-to-end data pipeline using Apache Airflow, PostgreSQL, and Docker.
It covers data ingestion, transformation, conditional workflows, file generation, and notifications using multiple DAGs.

---

## 🚀 Tech Stack

- Apache Airflow 2.8.0
- PostgreSQL 13
- Python 3.8
- Docker & Docker Compose
- Pandas
- SQLAlchemy

---

## 📂 Project Structure
```
airflow-data-workflows/
│
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
│
├── data/
│   ├── input/
│   └── output/
│
├── docker-compose.yml
└── README.md

```

## 🔁 DAG Overview

DAG 1: CSV → PostgreSQL Ingestion  
File: dag1_csv_to_postgres.py

- Creates table if not exists
- Truncates existing data
- Loads CSV data into PostgreSQL (raw_employee_data)
- Ensures idempotent ingestion

---

DAG 2: PostgreSQL Transformation  
File: dag2_data_transformation.py

- Reads data from PostgreSQL
- Applies transformations
- Writes transformed data to employees_transformed table

---

DAG 3: PostgreSQL → Parquet Export  
File: dag3_postgres_to_parquet.py

- Reads transformed table
- Writes Parquet files to data/output/
- Multiple files can be generated for multiple DAG runs

---

DAG 4: Conditional Workflow  
File: dag4_conditional_workflow.py

- Uses BranchPythonOperator
- Executes different paths based on weekday vs weekend
- Demonstrates branching and cleanup logic

---

DAG 5: Notification Workflow  
File: dag5_notification_workflow.py

- Triggers notification logic
- Sends success or failure alerts
- Uses trigger rules to avoid unnecessary execution

---

## 🐳 How to Run the Project

1. Start Containers  
docker-compose up -d

2. Initialize Airflow Database  
docker-compose run --rm airflow-init airflow db migrate

3. Access Airflow UI  

URL: http://localhost:8080  
Username: airflow  
Password: airflow

---

## 🗄 PostgreSQL Access

docker exec -it airflow_postgres psql -U airflow_user -d airflow_db

Useful commands:

\\dt  
SELECT * FROM raw_employee_data;  
SELECT * FROM employees_transformed;

---

## ✅ Key Features Demonstrated

- DAG dependencies and sequencing
- Database connections using Airflow hooks
- Idempotent pipeline design
- Conditional task execution
- File generation using Parquet
- Notification handling
- Dockerized Airflow setup

---

## 📌 Notes

- Multiple green runs and multiple output files are expected when DAGs are triggered manually
- Skipped tasks in conditional DAGs indicate correct branching behavior
- All workflows were tested successfully using the Airflow UI

---

## 🏁 Conclusion

This project showcases a complete, production-style Airflow pipeline with multiple DAGs handling ingestion, transformation, branching, file generation, and notifications in a modular and scalable way.
