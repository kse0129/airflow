from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="dags_task_branch_decorator",
    start_date=datetime(2024, 3, 1),
    schedule=None,
    catchup=False
) as dag:
    
    @task.branch(task_id='python_branch_task')
    def select_random():
        from random import choice
        item_list = ['A', 'B', 'C']
        selected_item = choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        else: return ['task_b', 'task_c']

    def common_func(**kwargs):
        print(kwargs.get('selected'))

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    select_random() >> [task_a, task_b, task_c]