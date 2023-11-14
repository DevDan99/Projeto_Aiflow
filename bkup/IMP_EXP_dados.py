from airflow import DAG 
from airflow.operators.python import PythonOperator 
import datetime
from minio import Minio
import io
import requests
import base64

def importa():
    data= datetime.date.today().year #pega o ano atual da data.
    st= str(data) #transforma o ano atual em string
    url= "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_"+st+".csv" #pega a url do ano atual, fazendo o ano da string st ser sempre o atual
    req = requests.get(url, verify=False) #abre a url, consequentemente faz o donwload do arquivo.
    resp = req.content # guarda o conteudo do donwload.
    csv_64= base64.b64encode(resp).decode('utf-8')#codifica o csv para string, para que ele possa ser recebido na proxima task.
    return csv_64

#Decodifica o arquivo de volta para bytes e realiza o upload desse arquivo para o banco MinIO.
def up_minio(ti):
    csv_64= ti.xcom_pull(task_ids= 'importa')

    csv= base64.b64decode(csv_64)

    minio_client = Minio(
        '10.2.0.28:9000',
        access_key= 'AirFlw',
        secret_key= 'dBCvoOhizLpyvFZFWXA7ceqIo4J7XJYRhyD9yPto',
        secure= False
    )

    bukect_name= 'dadospublicos'
    file_name= 'IMP_dados.csv'

    minio_client.put_object(
        bukect_name,
        file_name,
        io.BytesIO(csv),
        len(csv),
        content_type= 'text/csv'
    )

def exportaxp():
        dataex= datetime.date.today().year
        stex= str(dataex)
        urlex= "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_"+stex+".csv"
        reqex= requests.get(urlex, verify=False)
        respex= reqex.content
        csv_b64ex= base64.b64encode(respex).decode('utf-8')
        return csv_b64ex
    
def up_minioEX(ti): 
    csvex= ti.xcom_pull(task_ids= 'exportaxp')

    csv = base64.b64decode(csvex)

    minio_client= Minio(
        '10.2.0.28:9000',
        access_key= 'AirFlw',
        secret_key= 'dBCvoOhizLpyvFZFWXA7ceqIo4J7XJYRhyD9yPto',
        secure= False
    )

    bukect_name= 'dadospublicos'
    file_name= 'EXP_dados.csv'

    minio_client.put_object(
        bukect_name,
        file_name,
        io.BytesIO(csv),
        len(csv),
        content_type='text/csv'
    )
    
with DAG('IMP_EXP_Dag',start_date= datetime.datetime(2023,10,5), schedule_interval= '0 9 * * 1', catchup=False) as dag:
    
    importa = PythonOperator(
        task_id= 'importa',
        python_callable= importa
    )

    up_minio = PythonOperator(
        task_id = 'up_minio',
        python_callable= up_minio
    )

    exportaxp = PythonOperator(
        task_id= 'exportaxp',
        python_callable= exportaxp
    )

    up_minioEX = PythonOperator(
        task_id= 'up_minioEX',
        python_callable= up_minioEX
    )

importa >> up_minio >> exportaxp >> up_minioEX