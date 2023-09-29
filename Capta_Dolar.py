from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd 
import json 
import requests #test



def Capta_dolar (): #faz requisição HTTP e obtem o JSON
    url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='USD'&@dataInicial='01-01-2022'&@dataFinalCotacao='12-31-2090'&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    response = requests.get(url)
    json= response.json()
    return json 

def Transforma_CSV(ti): # Transforma o JSON em CSV
    json= ti.xcom_pull(task_ids='Capta_dolar')
    df= pd.DataFrame(json)
    CSV= df.to_csv(index=False)
    return CSV

def Upload_Minio(ti):
    CSV = ti.xcom_pull(task_ids= 'Transforma_CSV')
    
    config = {
        "dest_bucket": "processed",
        "minio_endpoint": "127.0.0.1",
        "minio_user": "Airflow",
        "minio_password": "Airflow"
    } #configura a conexao com MinIO
    
    minio_client = Minio(
        config["minio_endpoint"],
        access_key=config["minio_user"],
        secret_key=config["minio_passaword"],
        secure=False
    ) # faz a conexão com Minio
    
    bucket_name= 'bucketesteversionado'
    file_name= 'Cot_dolar.csv'

    minio_client.put_object(
        bucket_name,
        file_name,
        CSV.encode('utf-8'),
        len(CSV)
    ) # realiza o Upload para o Bucket no MinIo

with DAG('Capta_Dolar',start_date= datetime(2023,9,27), schedule_interval='@daily', catchup= False) as dag:

    Capta_dolar= PythonOperator(
        task_id= 'Capta_dolar',
        python_callable= Capta_dolar 
    )

    Transforma_CSV= PythonOperator(
        task_id= 'Transforma_CSV',
        python_callable= Transforma_CSV
    )

    Upload_Minio= PythonOperator(
        task_id= 'Upload_Minio',
        python_callable= Upload_Minio

    )

    Capta_dolar >> Transforma_CSV >> Upload_Minio