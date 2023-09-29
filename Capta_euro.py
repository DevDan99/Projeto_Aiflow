from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd 
import requests

def Capta_euro():
    url= "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='EUR'&@dataInicial='01-01-2022'&@dataFinalCotacao='12-31-2090'&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    response= requests.get(url)
    json= response.json()
    return json

def Transforma_csv(ti):
    json= ti.xcom_pull(task_ids= 'Capta_euro')
    df= pd.DataFrame(json)
    csv= df.to_csv(index=False)
    return csv


def Upload_minio(ti):
    csv= ti.xcom_pull(task_ids= 'Transforma_csv'),
    


with DAG('Capta_euro', start_date=datetime(2023,9,1), schedule_interval='@daily', catchup=False) as dag:
    Capta_euro = PythonOperator(
        task_id= 'Capta_euro',
        python_callable= Capta_euro
    )

    Transforma_csv = PythonOperator(
        task_id= 'Transforma_csv',
        python_callable= Transforma_csv
    )

    Upload_minio = PythonOperator(
        task_id= 'Upload_minio',
        python_callable= Upload_minio
    )

    Capta_euro >> Transforma_csv >> Upload_minio