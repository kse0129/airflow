from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow import DAG
import pendulum

with DAG(
    dag_id="dags_trigger_rule_eg2",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    @task.branch(task_id='branch')
    def random_branch():
        from random import choice

        item_list = ['A', 'B', 'C']
        selected_item = choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        else:
            return 'task_c'
        
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo task_a success'
    )

    @task(task_id='task_b')
    def task_b():
        print("task_b success")
    
    @task(task_id='task_c')
    def task_c():
        print("task_c success")
    
    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print("task_d success")

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()

