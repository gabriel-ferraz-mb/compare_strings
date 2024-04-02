# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 15:03:12 2023

@author: gabriel.ferraz
"""

import geopandas as gpd
from sqlalchemy import create_engine
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import numpy as np
from geoalchemy2 import Geometry

usuario = 'gabriel'
senha = ''
host = ''
porta= '5432'
banco = 'postgres'
global engine
dbschema='mq' # Searches left-to-right

engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}',
    connect_args={'options': '-csearch_path={}'.format(dbschema)})

meso = gpd.read_file(r'C:\Projetos\GeoDB\BR_Mesorregioes_2022.shp')
micro = gpd.read_file(r'C:\Projetos\GeoDB\BR_Microrregioes_2022.shp')
mun = gpd.read_file(r'C:\Projetos\GeoDB\BR_Municipios_2022.shp') 
pais = gpd.read_file(r'C:\Projetos\GeoDB\BR_Pais_2022.shp')
imediatas = gpd.read_file(r'C:\Projetos\GeoDB\BR_RG_Imediatas_2022.shp')
intermediarias = gpd.read_file(r'C:\Projetos\GeoDB\BR_RG_Intermediarias_2022.shp')
uf= gpd.read_file(r'C:\Projetos\GeoDB\BR_UF_2022.shp')

meso.to_postgis('GEO_MESO_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

micro.to_postgis('GEO_MICRO_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

mun.to_postgis('GEO_MUN_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

pais.to_postgis('GEO_PAIS_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

imediatas.to_postgis('GEO_RGIMEDIATAS_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

intermediarias.to_postgis('GEO_RGINTERMEDIARIAS_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

uf.to_postgis('GEO_UF_22', engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4674)})

sparkbase = pd.read_csv(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\MQ_SOLUTIONS\SparkProdutos.csv', sep = ';')
sparkbase.to_sql('saprk_produtos', engine, if_exists = 'replace')
