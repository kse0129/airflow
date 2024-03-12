from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow import DAG
import pendulum

with DAG(
    dag_id="dags_trigger_rule_eg1",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    pass