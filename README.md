## FOREX data project

In this project I will create an ETL pipeline, that will process open 
data on forex spot rates, balance of payments and interest rates. (The latter were found redundand later)

The subsequent data will be used to for forecasting and the dashboard.

# Stage 1: ELT process

This was the most important and the most time consuming stages of them all.
In this stage I prepared the data: cleaned, picked the correct dimensions and gathered encodings.
This process was wrapped in an Airflow DAG. This was done so that in future, I could create an automated
pipeline reusing a lot of the code.

For data extraction I used Python, however for most data cleaning I used SQL.

# Stage 2: Analysis

This stage was done in two parts:
- Quality check. I made sure, that the data was accurate, that there were no bad SQL joins done, no data discarded for no reason.
- Time-series/correlation analysis. I wanted to make sure, that there was at least some correlation between the variables, I picked. Furthermore I checked time-series stationarity and seasonality.

For these tasks I was more keen on using R.

# Stage 3: Modelling

For the model I tried many statistical and ML models, that produced reasonable results. The models, that I experimented with:
1. ARIMA
2. Holt's smoothing
3. GBRT (the XGBoost implementation)
4. LSTM

# The result

