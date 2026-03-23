import requests
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

api_key = '7887ddab93ea08e8e5026a996fb76bc1'
url = 'https://api.openweathermap.org/data/2.5/weather'

params = {
    'q': 'Cachoeiro de Itapemirim,BR',
    'units': 'metric',
    'appid': api_key
}

#O parâmetro url é do tipo string e essa função retorna uma lista, por isso o -> list
def extract_weather_data(url:str, params) -> list:
    response = requests.get(url, params=params)
    data = response.json() #transformando a resposta da api em um dicionário python

    if response.status_code != 200:
        logging.error(f"Erro na requisição: {response.status_code} - {response.text}")
        return []
    
    if not data:
        logging.warning("Nenhum dado retornado")
        return []

    output_path = 'data/weather_data.json'
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)

    logging.info(f"Arquivo salvo em {output_path}")
    return data

extract_weather_data(url, params)