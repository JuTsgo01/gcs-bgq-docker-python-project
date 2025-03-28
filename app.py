import requests
import os
import base64
import json
import logging
import pandas as pd
import datetime
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from flask import Flask, jsonify

load_dotenv()

credentials_json = base64.b64decode(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')).decode('utf-8')
credentials_info = json.loads(credentials_json)

CREDENCIALS = service_account.Credentials.from_service_account_info(credentials_info)

app = Flask(__name__)

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class GetApi:
    
    def __init__(self):
        self.TOKEN = os.getenv('TOKEN')
        self.url = 'https://api.aviationstack.com/v1/flights'

    def __request_api(self):
        
        params = {  
            'access_key': self.TOKEN,
            'offset': 0,
            'status': 'landed'
        }
    
        try:
            response = requests.get(self.url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f'Error: {e} na função "__request_api" linha 19')
            raise
    
    def __return_fetch_api(self) -> list:

        response = self.__request_api()
        
        if response.status_code != 200:
            logging.error(f'Erro: API retornou {response.status_code}')
            raise ValueError(f'Falha na requisição: {response.status_code}')
        
        try:
            response_json = response.json()
            return response_json.get('data', [])
    
        except (ValueError, KeyError) as e:
            logging.error(f'Erro na conversão para JSON na função: {e}')
            raise

    def create_csv_file(self):   
        
        data = self.__return_fetch_api()
        
        if not data:
            logging.error('Erro: "return_fetch_api" retornou None ou vazio')
            raise ValueError('Erro: "return_fetch_api" retornou None ou vazio')

        try:
            data_json_normalized = pd.json_normalize(data)
            
            if data_json_normalized.empty:
                logging.warning('Erro: Colunas esperadas não estão presentes no df ou o df está vazio.')
                return ValueError('Erro Dataframe Vazio')
            
            logging.info(f'Dados rescebidos: {data_json_normalized}')
            return data_json_normalized

        except Exception as e:
            logging.error(f'Erro no tratamento do Dataframe na função "__createPandasDf" linha 48: {e}')
            raise
    

class InsertData:
    
    def __init__(self):
        self.BUCKET = os.getenv('BUCKET')
        self.CREDENTIAL = CREDENCIALS
        
    def insert_data(self, dataframe):
        
        hoje = datetime.date.today().strftime('%Y-%m-%d')
        client = storage.Client(credentials=self.CREDENTIAL)
        bucket = client.bucket(self.BUCKET)
        
        nome_blob = f'data-voos-{hoje}.csv'
        
        file_content = dataframe.to_csv(index=False, sep=';')
        
        bucket.blob(nome_blob).upload_from_string(file_content, 'text/csv')
        
        logging.info(f'Arqurivo: {nome_blob} inserido com sucesso no bucket {self.BUCKET}')
   
   
@app.route('/')
def run_task():
    logging.info("Iniciando a função run_task")
    try:
        get_api = GetApi()
        insercao_de_dados = InsertData()
        insercao_de_dados.insert_data(get_api.create_csv_file())
        
        logging.info("Dados inseridos com sucesso")

        return jsonify({'Status': 'Sucesso', 'Mensagem': 'Dados inseridos com sucesso'}), 200
        
    except Exception as e:
        logging.error(f'Erro {e}')
        return jsonify({'Status': 'Erro', 'Mensagem': 'Dados não foram inseridos com sucesso'}), 500
        
if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host="0.0.0.0", port=port)
        
        
    