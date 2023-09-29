# importando Libs
from airflow import DAG #Importa a lib DAG, apenas a parte do airflow
from datetime import datetime #lib data e hora
from airflow.operators.python import PythonOperator, BranchPythonOperator #operadoradores do airflow, usados aqui para condicional 
from airflow.operators.bash import BashOperator #verifica o resultado da condicional
import pandas as pd #lib usada para trativas de dados
import requests 
import json 

#criando as funções:
#A função entra na url, capta um arquivo json, faz um response, que tras a responta da requisição do get da url, transforma o response em deta frame e depois realiza uma contagem do 
#DF, por último retona essa contagem.
def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df =pd.DataFrame(json.load(response.content))
    qtd = len(df.index)
    return qtd

# Função que vai fazer a operação logica do resultado
# Parametro ti, Task instance, podendo utilizar alguns metodos e o Xcom é um deles.
# Xcom pega informações de outra task

def e_valido(ti):
    qtd = ti.xcom_pull(task_id = 'captura_conta_dados')
    if (qtd > 100):
        return 'Valido'
    return 'Nvalido'

# Ciando Dags - Directed Acyclic Graphs (Dags é um conjunto de task)
# Tasks são tarefas dentro das Dags, elas são interligadas e aciclicas.
# Alocando a Dag
#Os parametros adcionados são: o ID da DAG = Treino, Data de inicio para starta a DAG = Start_date, Intervalo de execução da DAG = Schedule_inteval, e o catchup como false,
#esse paramnetro cria a DAG apenas para ultima execulção. 

with DAG('Treino',start_date= datetime(2023,9,1), schedule_interval= '30 * * * *', catchup= False) as Dag: 
    # Criando a task
    # aqui eu uso o PythonOperator para capturar os dados diacordo com os parametros qeu são:
    #Task_ai = nome da tarefa
    #pythn_callable para transformar o resultado da função, captura_conta_dados
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable= captura_conta_dados
    )

   #Task para validar se a quantidade é valida ou não.
    e_valido = BranchPythonOperator (
        task_id = 'e_valido',
        python_callable= e_valido
    )

     #Task para retornar mensagem depois da validação

    Valido = BashOperator(
        task_id = 'Valido',
        bash_command= "echo 'QTD OK'"
    )

    Nvalido = BashOperator(
      task_id = 'Nvalido',
      bash_command= "echo 'QTD Fora'"
    )

captura_conta_dados >> e_valido >> [Valido, Nvalido]