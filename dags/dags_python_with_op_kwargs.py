from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.common_python import regist

import datetime
import pendulum
import random

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    regist_t1 = PythonOperator(
        task_id="regist_t1",
        python_callable=regist,
        op_args=["kse", "man", "KR", "Seoul"],
        op_kwargs={"email":"zxc123@naver.com", "phone":"010-1234-1234"}
    )