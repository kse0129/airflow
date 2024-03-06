import datetime
import pendulum
import random

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.common_python import get_sftp


with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=get_sftp
    )

    py_t1