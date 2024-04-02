# -*- coding: utf-8 -*-
"""
Created on Mon Aug 21 21:07:04 2023

@author: gabriel.ferraz
"""

from sqlalchemy import create_engine
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

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

cnxn = pyodbc.connect('')
cursor = cnxn.cursor()


# engine_SQLServer  = create_engine('mssql+pyodbc://leitura@mqazure:mq!@#2023@mqazure.database.windows.net/BancoContatos')

# cnxn = pyodbc.connect("DRIVER=Devart ODBC Driver for SQL Server;Data Source=mqazure.database.windows.net;Initial Catalog=BancoContatos;User ID=leitura@mqazure;Password=mq!@#2023")

# tblCadastroContatos = pd.read_sql("select * from dbo.tblCadastroContatos", con=engine_SQLServer)

tblCadastroContatos = pd.read_sql("select * from dbo.tblCadastroContatos", cnxn)
totalCadastroContatos = len(tblCadastroContatos)
tblCategoriaInformante = pd.read_sql("select * from dbo.tblCategoriaInformante", cnxn)

tblCadastroContatos = pd.merge(tblCadastroContatos, tblCategoriaInformante, left_on='id_categoria_informante', right_on='id_categoria_informante')
tblCadastroContatos = tblCadastroContatos[tblCadastroContatos['grupo_categoria'].notnull()]
tblCadastroContatos = tblCadastroContatos[tblCadastroContatos['Status']!= 'descartado']
filterGrupoCategoria = len(tblCadastroContatos)

tblContato = pd.read_sql("select * from dbo.tblContato", cnxn)
tblDadosObtidosDefensivos = pd.read_sql("select * from dbo.tblDadosObtidosDefensivos", cnxn)
tblFabricantes = pd.read_sql("select * from dbo.tblFabricantes", cnxn)
tblProdutos = pd.read_sql("select * from dbo.tblProdutos", cnxn)

listSegmento = [10,11,16,17,1,6,7,18,19,20,49,54,82,84,92,93,94,95,96]
tblProdutos = tblProdutos[tblProdutos['IdSegmento'].isin(listSegmento)]

#tblSparkBaseProdutos[ 'PRODUTO']

#t = pd.merge(tblProdutos, tblSparkBaseProdutos, left_on= 'IdProdutoFarmTrak', right_on='PRODUTO', how = 'left')

tblContato['data coleta'] = pd.to_datetime(tblContato['data coleta'], errors='coerce')
# Import Pandas package

# =============================================================================
#  Análise dos últimos 6 meses
# =============================================================================

# Filter data between two dates
#tblContatoRecent = tblContato.loc[tblContato['data coleta'] >= '2023-01-01'][['Código', 'id_informante', 'data coleta']]
# Display

joinContatoCadastro = pd.merge(tblContato, tblCadastroContatos, left_on='id_informante', right_on = 'Id_informante', how = 'inner')
joinContatoCadastro.drop(columns = 'Id_informante', inplace = True)
joinDados = pd.merge(tblDadosObtidosDefensivos, joinContatoCadastro, left_on= 'id_contato', right_on= 'Código', how = 'inner')

database = pd.merge(joinDados, tblProdutos, left_on= 'id_produto', right_on='Id_Produto', how = 'inner')
database.drop_duplicates(subset=['id_informante', 'id_produto'], inplace = True)

GeToKyn = database[['IdGrupoEmpresarial','IdLocalCompraKYN']]
GeToKyn.drop_duplicates(inplace=True)
dictGeToKyn = dict(zip(GeToKyn.IdGrupoEmpresarial, GeToKyn.IdLocalCompraKYN))

database['idLocalCompraKYN_fromGE'] = database['IdGrupoEmpresarial'].map(dictGeToKyn) 


database['IdLocalCompraKYN'] = np.where(database['IdLocalCompraKYN'].isnull()\
                                                 ,database['idLocalCompraKYN_fromGE'], database['IdLocalCompraKYN'])
    
match = database[database['IdLocalCompraKYN'].notnull()]
noMatch = database[database['IdLocalCompraKYN'].isnull()]
matchCountMq = len(match[['id_informante', 'IdLocalCompraKYN']].drop_duplicates('IdLocalCompraKYN'))
noMatchCountMq = len(noMatch[['id_informante', 'IdLocalCompraKYN']].drop_duplicates('id_informante'))

kynLocalCompraInput = pd.read_csv(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\MQ_SOLUTIONS\BASE_LOCAL_COMPRA.csv',\
                             sep=';', encoding='ISO-8859-1 ', decimal=',')
    
    
kynLocalCompra = kynLocalCompraInput.groupby('IDLOCAL_COMPRA').agg({
    'AREA': 'sum',
    'VALOR_DE_MERCADO_MI_RS': 'sum'
}).reset_index()

kynLocalCompraMatch = pd.merge(kynLocalCompra, match[['id_informante', 'IdLocalCompraKYN']].drop_duplicates('IdLocalCompraKYN'), left_on='IDLOCAL_COMPRA', right_on='IdLocalCompraKYN', how = 'left')

noMatchKyn =   kynLocalCompraMatch[kynLocalCompraMatch['id_informante'].isnull()]#.drop_duplicates('IDLOCAL_COMPRA')  
matchKyn = kynLocalCompraMatch[kynLocalCompraMatch['id_informante'].notnull()]


totalArea = kynLocalCompra['AREA'].sum()
totalValue = kynLocalCompra['VALOR_DE_MERCADO_MI_RS'].sum()

matchArea = matchKyn['AREA'].sum()
noMatchArea = noMatchKyn['AREA'].sum()

matchValue = matchKyn['VALOR_DE_MERCADO_MI_RS'].sum()
noMatchValue = noMatchKyn['VALOR_DE_MERCADO_MI_RS'].sum()
