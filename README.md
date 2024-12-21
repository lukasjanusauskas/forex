## FOREX data project

In this project I will create an ETL pipeline, that will process open 
data on forex spot rates, balance of payments and interest rates.

The subsequent data warehouse will be used to demonstrate some analytics
and forecasting(with ARIMAx and XGboost as preliminary model candidates)

### Stage 1: ad hoc ETL
- Extract data from APIs with the SDMXCollector
- Store them in a Postgres database. 
- The model of the data will be a Star schema, based on the Kimball aproach.

### Stage 2: data engineering with Airflow

- Create an automated data pipeline with Airflow, that updates weekly.

### Stage 3: EDA and modelling

- Create a simple dashboard 
- Create two models:
    - ARIMAx model
    - GBRT model

### Stage 4: Monitoring the model

- Monitor the model with MLflow.
