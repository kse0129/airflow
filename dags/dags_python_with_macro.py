from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import pendulum
import datetime

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='task_using_macros',
          templates_dict={
            'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
            'end_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(day=1, days=-1)) | ds }}'})
    def get_datetime_macro(**kwargs):
        template_dict = kwargs.get('templates_dict') or {}
        if template_dict:
            start_date = template_dict.get('start_date') or 'None'
            end_date = template_dict.get('end_date') or 'None'

            print('start_date: ', start_date)
            print('end_date: ', end_date)
            

    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end']
        start_date = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        end_date = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(day=1) + relativedelta(days=-1)
        print(start_date.strftime('%Y-%m-%d'))
        print(end_date.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()

    