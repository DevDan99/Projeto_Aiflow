from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd 
import io
import json
import requests

def Capta_euro():
    url= "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='EUR'&@dataInicial='01-01-2022'&@dataFinalCotacao='12-31-2090'&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    response= requests.get(url) #pega a url
    dados= json.leads(response.content) #abre a url
    chave= ['cotacapCompra','cotacaoVenda','dataHoraCotacao','tipoBoletim'] #seleciona paenas as chaves necessário da lista de dicionario do json
    dic= [{key:d_orig[key] for key in chave} for d_orig in dados['value']] #Cria um novo dicionario sendo a key a variavel 'Chave' e o valor do dicionario original.
    df= pd.DataFrame(dic) #cria um data frame do dicionario novo
    return df

def Transforma_csv(ti):
    df= ti.xcom_pull(task_ids='Capta_euro')
    csv= df.to_csv(index=False) # converte o df para csv
    return csv


def Upload_minio(ti):
    csv= ti.xcom_pull(task_ids= 'Transforma_csv'),
     
    config = {
        "dest_bucket": "processed",
        "minio_endpoint": "127.0.0.1",
        "minio_user": "Airflow",
        "minio_password": "Airflow"
    } #cria um dicionario para as configurações
    
    minio_client = Minio(
        config["minio_endpoint"],
        access_key=config["minio_user"],
        secret_key=config["minio_passaword"],
        secure=False
    ) # faz a conexão com Minio usando o dicionario
    
    bucket_name= 'bucketesteversionado' #nome do bucket do minio
    file_name= 'Cot_euro.csv' #nome a ser dado para o arquivo
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