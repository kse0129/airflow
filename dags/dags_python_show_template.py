from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

import pendulum
import datetime

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 2, 28, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)
    
    python_t1 = PythonOperator(
        task_id="python_t1",
        python_callable=python_function1,
        op_kwargs={"start_date":"{{ data_interval_start | ds }}", "end_date":"{{ data_interval_end | ds }}"}
    )

    @task(task_id="python_t2")
    def python_function2(**kwargs):
        print(kwargs)
        print("ds: ", kwargs.get("ds"))
        print("ts: ", kwargs.get("ts"))
        print("data_interval_start: ", kwargs.get("data_interval_start"))
        print("data_interval_end: ", kwargs.get("data_interval_end"))
        print("task_instance: ", kwargs.get("task_instance"))


    @task(task_id="python_t3")
    def python_function3(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    def python_function4(**kwargs):
        print(kwargs)
        print("ds: ", kwargs.get("ds"))
        print("ts: ", kwargs.get("ts"))
        print("data_interval_start: ", kwargs.get("data_interval_start"))
        print("data_interval_end: ", kwargs.get("data_interval_end"))
        print("task_instance: ", kwargs.get("task_instance"))
    
    python_t4 = PythonOperator(
        task_id="python_t4",
        python_callable=python_function4,
    )

    python_t2 = python_function2()
    python_t3 = python_function3(start_date="{{ data_interval_start | ds }}", end_date="{{ data_interval_end | ds }}")
    python_t1 >> python_t2 >> python_t3 >> python_t4