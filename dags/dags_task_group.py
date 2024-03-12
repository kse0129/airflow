from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from airflow import DAG
import pendulum

with DAG(
    dag_id="dags_task_group",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id='first_group')
    def first_group():
        ''' 데커레이터를 이용한 첫 번째 그룹 '''
        
        @task(task_id='inner_function1')
        def inner_function1(**kwargs):
            print('첫 번째 task group의 첫 번째 task')
        
        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 task group의 두 번째 task'}
        )

        inner_function1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='데커레이터를 이용한 두 번째 그룹') as second_group:
        @task(task_id='inner_function1')
        def inner_function1(**kwargs):
            print('두 번째 task group의 첫 번째 task')
        
        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'두 번째 task group의 두 번째 task'}
        )
        inner_function1() >> inner_function2

    first_group() >> second_group