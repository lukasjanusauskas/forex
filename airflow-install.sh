pip install "apache-airflow[celery]==2.10.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow webserver -p 8082

airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --password pass \
  --email lukasjanusauskaslj@gmail.com 