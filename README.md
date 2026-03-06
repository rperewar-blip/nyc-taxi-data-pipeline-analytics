# NYC Taxi Data Pipeline & Analytics Dashboard

## Project Overview

This project builds a complete data pipeline and analytics dashboard using NYC taxi trip data.  
The pipeline automates data cleaning, storage, and reporting to enable data-driven insights into taxi operations.

The project demonstrates skills in:

- Data Engineering
- ETL Pipeline Development
- SQL Database Management
- Data Analysis
- Business Intelligence Dashboarding

---

## Architecture

The project follows this pipeline:

Raw Data → Python ETL Pipeline → PostgreSQL Database → SQL Analysis → Power BI Dashboard

---

## Technologies Used

Python  
Pandas  
PostgreSQL  
SQL  
Power BI  
Data Visualization  

---

## Dataset

NYC Taxi Trip Dataset

Contains information such as:

- Pickup & Dropoff Locations
- Trip Distance
- Fare Amount
- Passenger Count
- Payment Type
- Trip Time

---

## Data Pipeline Steps

### 1 Data Ingestion
Raw taxi trip data is loaded using Python and Pandas.

### 2 Data Cleaning
The pipeline performs:

- Date parsing
- Missing value handling
- Trip duration calculation
- Feature engineering

### 3 Data Storage
Cleaned data is inserted into a PostgreSQL database table.

### 4 Data Reporting
Python generates a daily revenue report automatically.

### 5 Data Visualization
Power BI dashboard provides insights into:

- Taxi demand patterns
- Revenue trends
- Payment methods
- Passenger distribution
- Distance vs fare relationship

---

## Dashboard Preview

![Dashboard](images/dashboard_preview.png)

---

## Key Insights

Taxi demand increases during evening hours.

Most trips involve 1 passenger.

Revenue increases proportionally with trip distance.

Credit card payments dominate taxi transactions.

Daily revenue shows consistent demand with minor fluctuations.

---

## How to Run the Pipeline

1 Install dependencies

pip install -r requirements.txt

2 Create PostgreSQL database

Database name:

taxi_analytics

3 Run SQL table creation script

create_table.sql

4 Run ETL pipeline

python pipeline/taxi_pipeline.py

---

## Author

Rishabh Perewar

Data Analytics | Data Engineering | Business Intelligence
