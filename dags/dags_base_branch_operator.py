from typing import Iterable
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

from airflow.utils.context import Context

with DAG(
    dag_id="dags_base_branch_operator",
    start_date=datetime(2024, 3, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            from random import choice
            print("context: ", context)
            item_list = ['A', 'B', 'C']
            selected_item = choice(item_list)
            if selected_item == 'A':
                return 'task_a'
            else: return ['task_b', 'task_c']

    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

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

    custom_branch_operator >> [task_a, task_b, task_c]
            
