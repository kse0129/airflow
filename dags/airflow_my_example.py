from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="airflow_my_example",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='push_by_return')
    def push_by_return():
        return 'success' # return 값 지정
      
    @task(task_id='pull_by_return')
    def pull_by_return(status, **kwargs):
        ti = kwargs.get['ti']
        print(ti.xcom_pull(key='return_value', task_ids='push_by_return'))
        print(status) 
    
    pull_by_return(push_by_return()) # 함수의 return 을 다른 함수의 인자로 전달