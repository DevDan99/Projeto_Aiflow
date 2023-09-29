from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd 
import io
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
    csv_bytes= csv.encode('utf-8') #converte str em bytes

    minio_client.put_object(
        bucket_name,
        file_name,
        io.BytesIO(csv_bytes), #passa o Byte em um objeto BytesIO
        len(csv_bytes)
    ) # realiza o Upload para o Bucket no MinIo

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