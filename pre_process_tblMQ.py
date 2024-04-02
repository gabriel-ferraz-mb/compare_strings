# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 16:43:25 2023

@author: gabriel.ferraz
"""

from sqlalchemy import create_engine
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import pycaret
# from pycaret.datasets import get_data
# data = get_data('france')
#from pycaret.arules import *
# =============================================================================
#  Connect to DB Postgres
# =============================================================================

usuario = 'gabriel'
senha = ''
host = ''
porta= '5432'
banco = 'postgres'
global engine
dbschema='mq' # Searches left-to-right

engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}',
    connect_args={'options': '-csearch_path={}'.format(dbschema)})

# m = pd.read_csv(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\ENTREGA PRODUTORES\url.csv', sep =';')
# m = m[['cultura', 'URL']]
# m.to_sql("mapas_url", engine, if_exists='replace')

import  pyodbc

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=mqazure.database.windows.net,1433;DATABASE=BancoContatos;UID=leitura@mqazure;PWD=mq!@#2023')
cursor = cnxn.cursor()


# engine_SQLServer  = create_engine('mssql+pyodbc://leitura@mqazure:mq!@#2023@mqazure.database.windows.net/BancoContatos')

# cnxn = pyodbc.connect("DRIVER=Devart ODBC Driver for SQL Server;Data Source=mqazure.database.windows.net;Initial Catalog=BancoContatos;User ID=leitura@mqazure;Password=mq!@#2023")

# tblCadastroContatos = pd.read_sql("select * from dbo.tblCadastroContatos", con=engine_SQLServer)

# =============================================================================
# Read data
# =============================================================================

tblCadastroContatos = pd.read_sql("select * from dbo.tblCadastroContatos", cnxn)
tblCadastroContatos = tblCadastroContatos[tblCadastroContatos['Status']!= 'descartado']


coords = tblCadastroContatos.copy()
coords = coords[['Id_informante', 'LatKYN', 'LogKYN']]
coords =coords[coords['LatKYN'].notna()].astype(int).astype(str)
coords['latitude']=coords['LatKYN'].str[:3]+'.'+coords['LatKYN'].str[3:]
coords['longitude']=coords['LogKYN'].str[:3]+'.'+coords['LogKYN'].str[3:]
coords.to_sql('tblInformanteCoords', engine, if_exists="replace")

tblContato = pd.read_sql("select * from dbo.tblContato", cnxn)
tblDadosObtidosDefensivos = pd.read_sql("select * from dbo.tblDadosObtidosDefensivos", cnxn)
tblFabricantes = pd.read_sql("select * from dbo.tblFabricantes", cnxn)
tblProdutos = pd.read_sql("select * from dbo.tblProdutos", cnxn)

listSegmento = [10,11,16,17,1,6,7,18,19,20,49,54,82,84,92,93,94,95,96]
tblProdutos = tblProdutos[tblProdutos['IdSegmento'].isin(listSegmento)]

tblCategoriaInformante = pd.read_sql("select * from dbo.tblCategoriaInformante", cnxn)
tblSparkBaseProdutos = pd.read_csv(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\MQ_SOLUTIONS\tbl_SparkBase_Produtos.csv', sep = ';')

#tblSparkBaseProdutos[ 'PRODUTO']

#t = pd.merge(tblProdutos, tblSparkBaseProdutos, left_on= 'IdProdutoFarmTrak', right_on='PRODUTO', how = 'left')

tblSparkBaseProdutos.drop_duplicates('PRODUTO', inplace= True)

tblContato['data coleta'] = pd.to_datetime(tblContato['data coleta'], errors='coerce')
# Import Pandas package

# =============================================================================
#  Análise dos últimos 6 meses
# =============================================================================

# Filter data between two dates
tblContatoRecent = tblContato.loc[tblContato['data coleta'] >= '2023-01-01'][['Código', 'id_informante', 'data coleta']]
# Display

joinContatoCadastro = pd.merge(tblContatoRecent, tblCadastroContatos, left_on='id_informante', right_on = 'Id_informante', how = 'inner')
joinContatoCadastro.drop(columns = 'Id_informante', inplace = True)
joinDados = pd.merge(tblDadosObtidosDefensivos, joinContatoCadastro, left_on= 'id_contato', right_on= 'Código', how = 'inner')

joinProdutos = pd.merge(joinDados, tblProdutos, left_on= 'id_produto', right_on='Id_Produto', how = 'inner')
joinFabricantes = pd.merge(joinProdutos, tblFabricantes, left_on='Id_Fabricante', right_on = 'Id_Fabricante', how = 'inner')
joinFabricantes.drop_duplicates(subset=['id_informante', 'id_produto'], inplace = True)

joinSparkBase = pd.merge(joinFabricantes, tblSparkBaseProdutos, left_on='IdProdutoFarmTrak', right_on ='PRODUTO', how = 'left')

database = pd.merge(joinSparkBase, tblCategoriaInformante, left_on='id_categoria_informante', right_on='id_categoria_informante')
#database['CD_UF'] = database['id_cidade'].apply(str).str[:2]
#head = database.head(100)
database['grupo_categoria'].fillna('Outros', inplace = True)

# send_db = database[['id_informante', 'NomeEmpresa', 'RazaoSocial', 'IdGrupoEmpresarial',
#        'GrupoEmpresarial', 'Endereco_x', 'Cep', 'Bairro', 'CNPJ', 'Site_x',
#        'id_categoria_informante', 'IdSetor', 'Id_CulturaFoco',
#        'IdPerfilComercial', 'Status', 'ReativarEm', 'Telefone_x', 'Fax', 'email',
#        'PessoaContato', 'id_cidade', 'eliminar', 'celular', 'CheckProdutos',
#        'CheckEndereco', 'CheckGrupoEmp', 'CheckSite', 'DataCheckSite',
#        'ObsCheckSite', 'IdResponsavelCheck', 'Pesquisa_Campo',
#        'NVendedores_Campo', '1º Contato Ok?', 'Categoria Já Definida?',
#        'Nome Alterado?', 'ObsGerais_x', 'WhatsApp', 'Facebook', 'Instagram',
#        'IdLocalCompraKYN', 'LatKYN', 'LogKYN']]

# send_db.drop_duplicates('id_informante', inplace = True)
# send_db.to_csv(f'C:/Projetos/tblCadastroContatos_filter.csv', encoding='UTF-8')
#n_informante = database.groupby(['id_cidade','grupo_categoria','IdGrupoEmpresarial'])['id_informante'].nunique().reset_index()

for i in database.columns:
     print(i)

# set(database['TXT_CULTURA'])
# # count_filiais = database.groupby('IdGrupoEmpresarial')['id_informante'].nunique().reset_index()
# count_filiais['id_informante'] = count_filiais['id_informante'].apply(str)

# count_filiais = count_filiais.groupby('id_informante').size().reset_index()#
# count_filiais['id_informante'] = pd.to_numeric(count_filiais['id_informante'])
# count_filiais.sort_values('id_informante', inplace = True)

#database.to_sql('mqDb', engine)
#n_informante.to_sql('coun_informantes_filiais_mun', engine, if_exists="replace")

# for c in database.columns:
#     print("'"+c+"',")


# count_categoria = database.groupby('grupo_categoria')['id_informante'].nunique().reset_index()
# count_categoria.to_sql('coun_categoria', engine)



# count_mun = database.groupby('id_cidade')['id_informante'].nunique().reset_index()
# count_mun.to_sql('coun_mun', engine)

bandeirasDb = database.drop_duplicates(['id_informante','NomeFabricante'])
bandeiras = bandeirasDb.groupby('id_informante', as_index=False).agg({'NomeFabricante': ', '.join})#.transform(lambda x: ', '.join(x)).reset_index()

bandeiras["BAYER"] = bandeiras["NomeFabricante"].str.lower().str.contains("bayer")
bandeiras["BASF"] = bandeiras["NomeFabricante"].str.lower().str.contains("basf")
bandeiras["SYNGENTA"] = bandeiras["NomeFabricante"].str.lower().str.contains("syngenta")
bandeiras["CORTEVA"] = bandeiras["NomeFabricante"].str.lower().str.contains("corteva")

cols = ["BAYER", "BASF", "CORTEVA","SYNGENTA"]

allowed_manufacturers = ["Bayer", "Basf", "Syngenta", "Corteva"]

# Function to filter and modify names
def filter_and_concatenate(names):
    filtered_names = [name for name in names if any(manufacturer in name for manufacturer in allowed_manufacturers)]
    return '/'.join(filtered_names)
def sort_list(lst):
    return sorted(lst)

# Apply the function to each row
bandeiras['FilteredNomeFabricante'] = bandeiras['NomeFabricante'].str.split().apply(filter_and_concatenate)
bandeiras['FilteredNomeFabricante'] =bandeiras['FilteredNomeFabricante'].str.replace(',','')
bandeiras['FilteredNomeFabricante'] = bandeiras['FilteredNomeFabricante'].apply(lambda x: x.split('/'))
bandeiras['FilteredNomeFabricante'] = bandeiras['FilteredNomeFabricante'].apply(set)
bandeiras['FilteredNomeFabricante'] = bandeiras['FilteredNomeFabricante'].apply(sort_list)

bandeiras['coun'] = bandeiras.NomeFabricante.str.count(",") +1

bandeiras[cols] = bandeiras[cols].astype(int)
bandeiras['big_count'] = bandeiras[cols].sum(axis = 1)

#list(bandeiras['FilteredNomeFabricante'])


bandeiras.to_sql('coun_bandeiras', engine, if_exists="replace")



# =============================================================================
#  Análise Histórica
# =============================================================================

joinContatoCadastro = pd.merge(tblContato, tblCadastroContatos, left_on='id_informante', right_on = 'Id_informante', how = 'inner')
joinContatoCadastro.drop(columns = 'Id_informante', inplace = True)
joinDados = pd.merge(tblDadosObtidosDefensivos, joinContatoCadastro, left_on= 'id_contato', right_on= 'Código', how = 'inner')

joinProdutos = pd.merge(joinDados, tblProdutos, left_on= 'id_produto', right_on='Id_Produto', how = 'inner')
joinFabricantes = pd.merge(joinProdutos, tblFabricantes, left_on='Id_Fabricante', right_on = 'Id_Fabricante', how = 'inner')
#joinFabricantes.drop_duplicates(subset=['id_informante', 'id_produto'], inplace = True)

joinSparkBase = pd.merge(joinFabricantes, tblSparkBaseProdutos, left_on='IdProdutoFarmTrak', right_on ='PRODUTO', how = 'left')

database = pd.merge(joinSparkBase, tblCategoriaInformante, left_on='id_categoria_informante', right_on='id_categoria_informante')
database['year'] = pd.DatetimeIndex(database['data coleta']).year
#database['CD_UF'] = database['id_cidade'].apply(str).str[:2]
databaseHist = database.copy()
databaseHist['grupo_categoria'].fillna('Outros', inplace = True)
databaseHist['NomeFabricante'] = np.where(databaseHist['NomeFabricante'].isin(['Corteva','Bayer','Basf','Syngenta']), databaseHist['NomeFabricante'], 'Outros')
databaseHist['year'] = pd.DatetimeIndex(databaseHist['data coleta']).year

bandeirasDbHist = databaseHist.drop_duplicates(['year','id_informante','NomeFabricante'])
bandeirasDbHist.sort_values('NomeFabricante', inplace = True)

bandeirasHist = bandeirasDbHist.groupby(['year','id_informante'], as_index=False).agg({'NomeFabricante': ', '.join})#.transform(lambda x: ', '.join(x)).reset_index()
#bandeirasHist['coun'] = bandeirasHist.NomeFabricante.str.count(",") +1
bandeirasHist["Bayer"] = bandeirasHist["NomeFabricante"].str.lower().str.contains("bayer")
bandeirasHist["Basf"] = bandeirasHist["NomeFabricante"].str.lower().str.contains("basf")
bandeirasHist["Syngenta"] = bandeirasHist["NomeFabricante"].str.lower().str.contains("syngenta")
bandeirasHist["Corteva"] = bandeirasHist["NomeFabricante"].str.lower().str.contains("corteva")

cols = ["Bayer", "Basf", "Corteva","Syngenta"]

bandeirasHist[cols] = bandeirasHist[cols].astype(int)
bandeirasHist['big_count'] = bandeirasHist[cols].sum(axis = 1)
set(bandeirasHist['NomeFabricante'])
#bandeiras.to_sql('coun_bandeiras', engine)
bandeirasPivot = pd.pivot(bandeirasHist, values = 'NomeFabricante', index = 'id_informante', columns='year')
bandeirasPivot.fillna('S.I.', inplace =True)

dfsL = []
for col in cols:
    d = bandeirasPivot.copy()
    for i in range (2019, 2024):
        d[i] = np.where(bandeirasPivot[i].str.contains(col), 1, 0)
    #d.to_sql('coun_' + col.lower() + '_hist', engine)
    dfsL.append(d)

hist_bandeiras = pd.concat(dfsL)
hist_bandeiras.to_sql('coun_bandeiras_hist', engine)

def remove_duplicates(l):
    return list(set(l))

totalBandeiras = database.groupby(['year','id_informante'], as_index=False).agg({'NomeFabricante': ', '.join})

totalBandeiras['FilteredNomeFabricante'] = totalBandeiras['NomeFabricante'].str.split().apply(filter_and_concatenate)
totalBandeiras['FilteredNomeFabricante'] =totalBandeiras['FilteredNomeFabricante'].str.replace(',','')
totalBandeiras['FilteredNomeFabricante'] = totalBandeiras['FilteredNomeFabricante'].apply(lambda x: x.split('/'))
totalBandeiras['FilteredNomeFabricante'] = totalBandeiras['FilteredNomeFabricante'].apply(remove_duplicates)
totalBandeiras['FilteredNomeFabricante'] = totalBandeiras['FilteredNomeFabricante'].apply(sort_list)
#totalBandeiras['FilteredNomeFabricante'] = totalBandeiras['FilteredNomeFabricante'].astype("string")

totalBandeiras['NomeFabricante'] = totalBandeiras['NomeFabricante'].str.split(', ')##.apply(sort_list).astype("string")
totalBandeiras['NomeFabricante'] = totalBandeiras['NomeFabricante'].apply(remove_duplicates)
totalBandeiras['NomeFabricante'] = totalBandeiras['NomeFabricante'].apply(sort_list)
#totalBandeiras['NomeFabricante'] = totalBandeiras['NomeFabricante'].astype("string")
totalBandeiras['coun'] = totalBandeiras.NomeFabricante.str.len()
totalBandeiras['big_coun'] = totalBandeiras.FilteredNomeFabricante.str.len()



totalBandeiras.to_sql('coun_bandeiras_hist_total', engine, if_exists='replace')
# bandeirasPivot['sequence'] = bandeirasPivot[[2019, 2020, 2021, 2022, 2023]].agg(' / '.join, axis=1)

# data = list(bandeirasPivot['sequence'].apply(lambda x:x.split(" / ") ))

# from mlxtend.preprocessing import TransactionEncoder
# from mlxtend.frequent_patterns import apriori, association_rules

# a = TransactionEncoder()
# a_data = a.fit(data).transform(data)
# df = pd.DataFrame(a_data,columns=a.columns_)
# df = df.replace(False,0)
# df = apriori(df, min_support = 0.2, use_colnames = True, verbose = 1)

# df_ar = association_rules(df, metric = "confidence", min_threshold = 0.01)
# df_ar