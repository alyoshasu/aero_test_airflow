import os
import json
import pandas as pd
import requests
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from sqlalchemy import create_engine, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import UUID

POSTGRES_URL = Variable.get('POSTGRES_URL')

default_args = {
    'start_date': days_ago(1),
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
    dag_id='cannabis_connector',
    description='A DAG to fetch and store cannabis data',
    schedule_interval= '0 */12 * * *',
    default_args=default_args,
    catchup = False # если нужно загрузить исторические данные, то нужно поставить соответствующу дату в default_args и catchup = True
) as dag:

    def fetch_data_from_api(**context):

        url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
        response = requests.get(url)
        data = response.text
        context['ti'].xcom_push(key='data', value=data)

    def save_data_to_pg(**context):

        context = context['ti'].xcom_pull(key='data', task_ids='preprocessing_stage.download_cannabis_dataset')
        df = pd.DataFrame(json.loads(context))
        df['datetime'] = dt.datetime.now()
        engine = create_engine(POSTGRES_URL) 
        df.to_sql(
            'cannabis', 
            engine, 
            schema='cannabis_data',
            if_exists='append', 
            index=False,
            dtype={
                'id':Integer,
                'uid':UUID,
                'strain':String,
                'cannabinoid_abbreviation':String,
                'cannabinoid':String,
                'terpene':String,
                'medical_use':String,
                'health_benefit':String,
                'category':String,
                'type':String,
                'buzzword':String,
                'brand':String,
                'datetime':DateTime
            }
        )
        

    start = BashOperator(
        task_id='start',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )

    with TaskGroup(group_id="preprocessing_stage") as preprocessing:
        
        create_cannabis_dataset = PythonOperator(
            task_id='download_cannabis_dataset',
            python_callable=fetch_data_from_api,
            dag=dag
        )

        save_cannabis_dataset = PythonOperator(
            task_id='save_cannabis_dataset',
            python_callable=save_data_to_pg,
            dag=dag
        )

        create_cannabis_dataset >> save_cannabis_dataset

    start >> preprocessing
