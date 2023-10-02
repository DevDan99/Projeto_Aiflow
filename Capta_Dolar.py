from airflow import DAG #lib da Dag no airflow
from datetime import datetime #lib para obter data e hora
from airflow.operators.python import PythonOperator #Lib para cria dag
from minio import Minio #lib para interagir com o Min IO
import pandas as pd #tratamento de dados
import requests #faz requisições HTML
import io # criar o arqui temporario na memoria
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

def Upload_Minio(ti):
    csv = ti.xcom_pull(task_ids= 'Transforma_CSV') #pega dados da task aterior
    
    config = {
        "dest_bucket": "processed",
        "minio_endpoint": "127.0.1.1",
        "minio_user": "admin",
        "minio_password": "admin"
    } #configura a conexao com MinIO
    
    minio_client = Minio(
        config["minio_endpoint"],
        access_key=config["minio_user"],
        secret_key=config["minio_password"],
        secure=False
    ) # faz a conexão com Minio
    
    bucket_name= 'bucketesteversionado'
    file_name= 'Cot_dolar.csv'
    csv_bytes= csv.encode('utf-8') #converte str em bytes

    minio_client.put_object(
        bucket_name,
        file_name,
        io.BytesIO(csv_bytes), #passa o Byte em um objeto BytesIO
        len(csv_bytes),
        content_type= 'text/csv'
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