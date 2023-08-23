from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os
from consultar_APISpotify  import main


# argumentos por defecto para el DAG
default_args = {
    'owner': 'AyelenD',
    'start_date': datetime(2023,8,22),
    'retries':3,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='Spotify_ETL',
    default_args=default_args,
    description='Agrega data de Spotify de forma diaria',
    schedule_interval="@daily",
    catchup=False
)


# Tareas
task_1 = PythonOperator(
    task_id='ETL_Spotify',
    python_callable=main,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)
# Definicion orden de tareas
task_1
