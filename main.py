import gspread
import pandas as pd
from google.oauth2 import service_account

#criando o caminho da credencial
credentials_file_path = "./key/credentials.json"

#criando credenciais para acessaar o banco de dados
credentials = service_account.Credentials.from_service_account_file(
    credentials_file_path
)

#id dentro do arquivo .json baixado do google big query
id_sheet = '1_CoRWpU13PN3WdHreAlVefx_hlivnunhhrhQZkFltDo'

#conectando fonte
gc = gspread.service_account(filename=credentials_file_path)

#verificando permissões
#print(gc.list_permissions(file_id=id_sheet))

#criando a worksheet
wks = gc.open_by_key(id_sheet)

#icrementando dentro de umalista vazia
wks_list = [sheet.title for sheet in wks.worksheets()]

#id do projeto do big query
project_id = "arctic-defender-408420"
#id do dataset
dataset_id = "google_sheets"
#nome da tabela que vamos salva no banco de dados do big query
table_name = "sales"

df_list = list() #lista iniciada vazia

#iterando sobre as abas
for sheet in wks_list:
    #tabela
    data = wks.worksheet(sheet).get_all_values()
    #criando tabela e cabeçalho
    df = pd.DataFrame(data=data[1:], columns=data[0])
    df_list.append(df) #criando lista de tabela
    print(df.head())

#unificando as tabelas
df_sales = pd.concat(df_list, axis=0, ignore_index=True)

#projeto.dataset.tabela
df_sales.to_gbq(
    credentials=credentials,
    destination_table=f"{project_id}.{dataset_id}.{table_name}",
    project_id=project_id,
    if_exists="replace",
    progress_bar=True
)