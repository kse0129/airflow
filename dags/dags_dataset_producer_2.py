from airflow.datasets import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_producer_1 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id='dags_dataset_producer_2',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_producer_1],
        bash_command='echo "producer_2 실행 완료"'
    )