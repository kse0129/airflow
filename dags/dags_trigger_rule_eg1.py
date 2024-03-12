from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow import DAG
import pendulum

with DAG(
    dag_id="dags_trigger_rule_eg1",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream1')
    def python_upstream1():
        raise AirflowException('upstream1 exception')
    
    @task(task_id='python_upstream2')
    def python_upstream2():
        print("upstream2 success")
    
    @task(task_id='python_downstream1', trigger_rule='all_done')
    def python_downstream1():
        print("downstream1 success")

    [bash_upstream_1, python_upstream1(), python_upstream2()] >> python_downstream1()