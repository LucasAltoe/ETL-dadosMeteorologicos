from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os
import pandas as pd
from dotenv import load_dotenv

# Garante que o Airflow ache seus scripts dentro do container
sys.path.insert(0, '/opt/airflow') 

from src.extract_data import extract_weather_data
from src.load_data import load_weather_data
from src.transform_data import data_transformations

# O caminho do .env dentro do container
env_path = Path('/opt/airflow/config/.env')
load_dotenv(env_path)

API_KEY = os.getenv('OPEN_WEATHER_API_KEY')
url = 'https://api.openweathermap.org/data/2.5/weather'
params = {
    'q': 'Cachoeiro de Itapemirim,BR',
    'units': 'metric',
    'appid': API_KEY
}

@dag(
    dag_id='weather_pipeline',
    default_args={
        'owner': 'lucas_altoe',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline ETL - Clima Cachoeiro de Itapemirim',
    schedule='0 */1 * * *',
    start_date=datetime(2026, 3, 22),
    catchup=False,
    tags=['weather', 'etl']
)
def weather_pipeline_dag():

    @task
    def extract_task():
        extract_weather_data(url, params)

    @task
    def transform_task():
        # Certifique-se que a pasta /opt/airflow/data existe no container
        df = data_transformations()
        path = '/opt/airflow/data/temp_data.parquet'
        df.to_parquet(path, index=False)
        return path

    @task
    def load_task(parquet_path):
        df = pd.read_parquet(parquet_path)
        load_weather_data('cachoeiro_weather', df)

    # Definindo a ordem e passando o caminho do arquivo via XCom implícito
    e = extract_task()
    t = transform_task()
    l = load_task(t)

    e >> t >> l

# ESSENCIAL: Chamar a função para registrar a DAG
weather_pipeline_instance = weather_pipeline_dag()
