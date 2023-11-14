import datetime
cont= 0
while cont < 3:
    d = datetime.date.today().year - cont
    st= str(d)
    url= "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_"+ st + ".csv"
    cont +=1 

print(url)

# def Trata_zip():
#     #obtem o contenudo da url (download direto), depois obter o arquivo baixado, cria uma alocação de memoria para o arquivo.
#     url = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2023.csv"
#     resp = requests.get(url, verify=False)
#     csv_content= resp.content
    
#     # with zipfile.ZipFile( zip_bytes, 'r') as Exp:
#     #     list_info= Exp.namelist()
#     #     csv_content = Exp.read(list_info[0]) #Lê o conteúdo do arquivo CSV e guarda na variavel e adciona o nome a ele.
#     csv_B64 = base64.b64encode(csv_content).decode('utf-8') #converte o arquivo para base64 (O airflow so consegue ler JSON no Xcom, por isso converto em JSON)
#     return csv_B64