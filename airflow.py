from pipeline import *
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain, cross_downstream

default_arguments = {'owner': 'AirBnb', 'start_date': days_ago(1)}
city_name='athens'
with DAG('airbnb_airflow_hdfs_ETL',
    schedule_interval = None,
    catchup = False,
    default_args = default_arguments
) as dag:
    etl_proc = PythonOperator(task_id='etl',
                                 python_callable=etl)
    modeling_proc = PythonOperator(task_id='modeling',
                                 python_callable=modeling)
    insights_proc = PythonOperator(task_id='insights',
                              python_callable=insights)
    combine_proc = PythonOperator(task_id='combine',
                                 python_callable=combine)

    etl_proc >> [modeling_proc, insights_proc] >> combine_proc
