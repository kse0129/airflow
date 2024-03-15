from airflow.datasets import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_producer_1 = Dataset("dags_dataset_producer_1")
dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id='dags_dataset_consumer_2',
    schedule=[dataset_producer_1, dataset_producer_2],
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ti.run_id}} && echo "producer_1와 producer_2가 모두 완료되면 실행"'
    )