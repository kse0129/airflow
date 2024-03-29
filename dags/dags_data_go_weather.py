from operators.data_go_kr_operator import DataGoKrCsvOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='data_go_weather',
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    short_weather = DataGoKrCsvOperator(
        task_id='short_weather',
        path='/opt/airflow/files/short_weather/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='short_weather.csv',
        provider='1360000',
        api_name=['VilageFcstInfoService_2.0', 'getUltraSrtNcst'],
        api_params={
            'serviceKey':'{{var.value.apikey_weather_data_go_kr}}',
            'numOfRows':"10",
            'pageNo':"1",
            'dataType':"JSON",
            'base_date':"20240314",
            'base_time':"0600",
            'nx':'55',
            'ny':'127'}
    )

    def insert_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        import pandas as pd
        print("postgres_conn_id: ", postgres_conn_id)
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                ti = kwargs['ti']
                df = pd.read_csv(ti.xcom_pull(task_ids='short_weather'))
                sql = 'INSERT INTO short_weather values (%s, %s, %s, %s, %s, %s);'
                for base_date, base_time, category, nx, ny, obsr_value in df.values:
                    cursor.execute(sql, (base_date, base_time, category, nx, ny, obsr_value))
                conn.commit()

    insert_postgres_with_hook = PythonOperator(
        task_id='insert_postgres_with_hook',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )

    short_weather >> insert_postgres_with_hook