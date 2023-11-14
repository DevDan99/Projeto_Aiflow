from airflow import DAG
from datetime import datetime 
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd 
import json
import requests 
import io

def Capta_euro():
    url= "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='EUR'&@dataInicial='01-01-2022'&@dataFinalCotacao='12-31-2090'&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    resposta= requests.get(url)
    dados= json.loads(resposta.content)
    chave= ['cotacaoCompra','cotacaoVenda','dataHoraCotacao','tipoBoletim']
    dic= [{key:d_orig[key] for key in chave} for d_orig in dados['value']]
    df= pd.DataFrame(dic)
    return df

def Transforma_euro(ti):
    df= ti.xcom_pull(task_ids= 'Capta_euro')
    csv= df.to_csv(index=False)
    return csv

def Update_minio(ti):
    csv= ti.xcom_pull(task_ids= 'Transforma_euro')

    config= {
        "minio_enpoit":"10.2.0.28:9000",
        "minio_user":"AirFlw",
        "minio_pass":"dBCvoOhizLpyvFZFWXA7ceqIo4J7XJYRhyD9yPto",
    }

    minio_client = Minio(
        config["minio_enpoit"],
        access_key= config["minio_user"],
        secret_key= config["minio_pass"],
        secure=False
    )

    bucket_name= 'dadospublicos'
    file_name= 'Capta_euro.csv'
    csv_bytes= csv.encode('utf-8')

    minio_client.put_object(
        bucket_name,
        file_name,
        io.BytesIO(csv_bytes),
        len(csv_bytes),
        content_type= 'Text/csv'
    )


with DAG('Capta_Euro',start_date= datetime(2023,10,2),schedule_interval='@daily',catchup= False) as dag:
    
    Capta_euro = PythonOperator(
        task_id= 'Capta_euro',
        python_callable= Capta_euro
    )

    Transforma_euro = PythonOperator(
        task_id= 'Transforma_euro',
        python_callable= Transforma_euro
    )

    Update_minio = PythonOperator(
        task_id= 'Update_minio',
        python_callable= Update_minio
    )

    Capta_euro >> Transforma_euro >> Update_minio