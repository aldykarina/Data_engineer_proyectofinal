import os
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import logging
from modules import DataConn , DataInformation, generate_email_content_and_subject
from dotenv import load_dotenv

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::MainModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

load_dotenv()

class MainOperator(PythonOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(MainOperator, self).__init__(
            python_callable=self.main,
            *args,
            **kwargs
        )

    def main(self, **context):
        user_credentials = {
            "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
            "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
            "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
            "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
            "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME'),
        }
    
    
        schema:str = "aldykarinacp_coderhouse"
        table:str = "stage_market_data_daily"
        API_KEY:str = os.getenv('API_KEY')
   
        data_conn = DataConn(user_credentials, schema, table)
        data_information = DataInformation() 

        try:
            data_conn.get_conn()
            data = data_information.get_all_data(API_KEY)
            data_conn.upload_data(data, table)
            logging.info(f"Data uploaded to -> {schema}.{table}")

        except Exception as e:
            logging.error(f"Not able to upload data\n{e}")
        finally:
            data_conn.close_conn()
    pass

## TAREAS
# argumentos por defecto para el DAG
default_args = {
    'owner': 'aldykarina',
    'start_date': datetime(2024,7,15),
    'email': ['aldytips@gmail.com'],
    'email_on_retry':True,
    'email_on_failure': True,
    'retries':2,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='MarketData_ETL',
    default_args=default_args,
    description='Agrega data de mercado financiero de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()     #path original.. home en Docker


task1 = BashOperator(
    task_id='primera_tarea',
    bash_command='echo Iniciando...'
)

task2 = MainOperator(
    task_id='main',
    dag=BC_dag
)

task3 = PythonOperator(
    task_id='generate_email_content_and_subject',
    python_callable= generate_email_content_and_subject,
    provide_context=True,
    dag=BC_dag
)

task4 = EmailOperator(
    task_id='send_email',
    to= Variable.get("to_address"),
    subject="{{ task_instance.xcom_pull(task_ids='generate_email_content_and_subject', key='email_subject') }}",
    html_content="{{ task_instance.xcom_pull(task_ids='generate_email_content_and_subject', key='estimates') }}",
    dag=BC_dag
)

task5 = BashOperator(
    task_id= 'tercera_tarea',
    bash_command='echo Proceso completado...'
)

task1 >> task2 >> task3 >> task4 >> task5 

