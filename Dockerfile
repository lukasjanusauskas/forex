FROM apache/airflow:2.10.4
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==2.10.4" -r /requirements.txt