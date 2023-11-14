from airflow import DAG #lib da Dag no airflow
from datetime import datetime #lib para obter data e hora
from airflow.operators.python import PythonOperator #Lib para cria dag
#from minio import Minio #lib para interagir com o Min IO
import pandas as pd #tratamento de dados
import requests #faz requisições HTML
#import io # criar o arqui temporario na memoria
import json


def Capta_dolar (): #faz requisição HTTP e obtem o JSON
    url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='USD'&@dataInicial='01-01-2022'&@dataFinalCotacao='12-31-2090'&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    response = requests.get(url)
    dados= json.loads(response.content) #abrindo o arquivo que foi pego
    key_select= ['cotacaoCompra','cotacaoVenda','dataHoraCotacao','tipoBoletim'] #cria lista de chaves escolhidas e organizar os dados dos mesmos tipos
    filtro_dados= [{key: D_orig[key] for key in key_select} for D_orig in dados['value']] #para cada dicionario no Json, é criado um novo dicionario, contendo apenas as chaves filtradas
    df= pd.DataFrame(filtro_dados)
    return df

def Transforma_CSV(ti): # Transforma o JSON em CSV
    df= ti.xcom_pull(task_ids='Capta_dolar')
    csv= df.to_csv(index=False)
    return csv

def Verifica_CSV(ti):
    csv_content = ti.xcom_pull(task_ids='transforma_csv')
    if csv_content:
        print("Arquivo transformado em CSV com sucesso!")
    else:
        print("Falha ao transformar o arquivo em CSV.")

with DAG('Teste', start_date= datetime(2023,10,23),schedule_interval='@daily', catchup= False) as Dag:
    Capta_dolar = PythonOperator(
        task_id = 'Capta_dolar',
        python_callable= Capta_dolar
    )

    Transforma_CSV = PythonOperator(
        task_id= 'Transforma_CSV',
        python_callable= Transforma_CSV
    )

    Verifica_CSV = PythonOperator(
        task_id = 'Verifica_CSV',
        python_callable= Verifica_CSV
    )

    Capta_dolar >> Transforma_CSV >> Verifica_CSV