Overview - E-commerce airflow ETL pipeline
========
<img width="6834" height="2368" alt="Blank diagram - Page 1 (3)" src="https://github.com/user-attachments/assets/e2e9842f-f700-4cec-a700-847b33fc4892" />

Modern ETL pipeline built with Apache Airflow, Docker, and PostgreSQL, extracting live product data from a public e-commerce API , transforming and storing it in a postgres datatbase for analytics. Using data engineering best practices, modular DAG design, and real-world pipeline orchestration.

Project Contents
================

This project demonstrates a production-ready ETL pipeline that:

- Extracts product data from an open eCommerce REST API

- Transforms the JSON data into a clean relational structure

- Loads it into a PostgreSQL database for analytics and BI consumption

- Schedules and monitors the workflow using Apache Airflow

Data Stack
===========================

| Component                       | Purpose                                    |
| ------------------------------- | ------------------------------------------ |
| **Apache Airflow**              | DAG scheduling, task dependency management |
| **PostgreSQL**                  | Destination database for transformed data  |
| **Docker Compose**              | Containerized local development            |
| **Python (requests, psycopg2)** | API integration and transformation logic   |
| **Astronomer CLI (`astro`)**    | Simplified Airflow environment management  |


Deploy Your Project to Astronomer
=================================
API Source

Endpoint: https://api.freeapi.app/api/v1/public/randomproducts

Type: RESTful JSON API

Pagination: ?page=<num> supported

Data: Product metadata (id, title, category, price)


Future Improvements
=======


✅ Add data quality checks (e.g. Great Expectations)

✅ Add S3 data lake stage before loading to Postgres

🕓 Add dbt models for analytics

🕓 Integrate Airbyte for connector-based ingestion 

Author 
=======
Farouq Omar 
Business Intelligence / Analytics Engineer

