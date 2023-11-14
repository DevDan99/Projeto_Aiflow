from airflow import DAG 
from airflow.operators.python import PythonOperator
from minio import Minio
#from datetime import datetime 
from io import StringIO
import datetime
import io #manipulação com arquivo em memoria
import requests #lida com requisição de HTTP
import base64 #converte dados binarios em texto, XML, JSON.
import pandas as pd

def Trata_csv():
    cont= 0
    df_concat = pd.DataFrame()
    while cont < 2:
        data= datetime.date.today().year - cont
        st= str(data)
        url= "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_"+ st + ".csv"
        req= requests.get(url, verify=False)
        df_novo = pd.read_csv(io.BytesIO(req.content))
        df_concat = pd.concat([df_concat, df_novo], ignore_index= True)
        cont += 1
    csv= df_concat.to_csv(index=False)
    return csv
    
def Upload_minio(ti):
    csv= ti.xcom_pull(task_ids='Trata_csv')

    minio_client = Minio(
        '10.2.0.28:9000',
        access_key='AirFlw',
        secret_key='dBCvoOhizLpyvFZFWXA7ceqIo4J7XJYRhyD9yPto',
        secure= False
    )

    bucket_name= 'dadospublicos'
    file_name= 'IMP_232221.csv'
    csv_enc= csv.encode('utf-8')

    minio_client.put_object(
        bucket_name,
        file_name,
        io.BytesIO(csv_enc),
        len(csv_enc),
        content_type= 'text/csv'
    )


with DAG('IMP_comex', start_date= datetime.datetime(2023,10,1), schedule_interval='@daily', catchup= False) as Dag:

    Trata_csv = PythonOperator(
        task_id= 'Trata_csv',
        python_callable= Trata_csv
    )
 
    Upload_minio = PythonOperator(
        task_id= 'Upload_minio',
        python_callable= Upload_minio
    )

    Trata_csv >> Upload_minio