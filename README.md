## FOREX data project

In this project I will create an ETL pipeline, that will process open 
data on forex spot rates, balance of payments and interest rates.

The subsequent data warehouse will be used to prepare data for forecasting and 
model monitoring.

### Stage 1: ad hoc ETL
- Extract data from APIs with the SDMXCollector
    - At this stage the SDMXCollector class helps to build URLS for API requests with small , however with automatic metadata scraping, this will be a much more significant tool for this project in the future.
- Store them in a Postgres database. 
    - At this stage the database will be local, however later this will be migrated to Airflow
- The model of the data will be a Star schema, based on the Kimball aproach.
    - Dimension tables are auto-generated with Python and will be improved with metadata sccraping.

### Stage 2: data engineering with Airflow

- Improvements to the SDMXCollector
    - Scrape metadata
    - Automate URL building, maybe some OOP stuff and add inheritance
- Create an automated data pipeline with Airflow, that updates weekly.
    - Incoreporate the new SDMXCollector
    - Create separate DAGs for initialization and updating
    - Accordingly makes API calls

### Stage 3: EDA and modelling

- Create a simple dashboard 
- Create two models:
    - ARIMAx model(if possible)
    - GBRT model(main candidate)

### Stage 4: Monitoring the model

- Monitor the model.
