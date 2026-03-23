from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path('/opt/airflow/config/.env')
load_dotenv(dotenv_path=env_path, override=True)

host = 'postgres'

def get_engine():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_DATABASE')
    logging.info(f'Conectando com user={user}, database={database}')
    return create_engine(
        f'postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}'
    )

def load_weather_data(table_name: str, df: pd.DataFrame):
    engine = get_engine()
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info('Dados carregados com sucesso!')
        with engine.connect() as conn:
            df_check = pd.read_sql(f'SELECT * FROM {table_name}', con=conn)
            logging.info(f'Total de registros na tabela: {len(df_check)}')
    except Exception as e:
        logging.error(f'Erro ao carregar dados no banco: {e}')
