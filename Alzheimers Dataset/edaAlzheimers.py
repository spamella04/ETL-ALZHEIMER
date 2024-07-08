import configparser
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns


#Instancia de la clase ConfigParser
config = configparser.ConfigParser() 
config.read('configDatabase.ini')


server = config['database']['server']
database = config['database']['database']
trusted_connection = config['database'].get('trusted_connection', 'no')


#Autenticación de Windows
if trusted_connection.lower() == 'yes':
    connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
else:
    #Autenticación de SQL Server
    username = config['database']['username']
    password = config['database']['password']
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

# Conexión SQLAlchemy
engine = create_engine(connection_string)

query_tablas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
df_tablas = pd.read_sql(query_tablas, engine)

for tabla in df_tablas['TABLE_NAME']:
    print(f"Información para la tabla '{tabla}':")
    
    
    query_filas = f"SELECT COUNT(*) AS Num_Filas FROM {tabla}"
    df_filas = pd.read_sql(query_filas, engine)
    print("Número de filas:", df_filas['Num_Filas'][0])
    
    
    query_columnas = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabla}'"
    df_columnas = pd.read_sql(query_columnas, engine)
    print("Número de columnas:", len(df_columnas))
    print("Información de las columnas:")
    print(df_columnas)
    
   
    query_nulos = f"""
    SELECT {', '.join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_null_count" for col in df_columnas['COLUMN_NAME']])}
    FROM {tabla}
    """
    df_nulos = pd.read_sql(query_nulos, engine)
    print("Conteo de valores nulos por columna:")
    print(df_nulos)
    
   
    query_duplicados = f"""
    SELECT COUNT(*) AS Num_Filas_Duplicadas
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(df_columnas['COLUMN_NAME'])} ORDER BY (SELECT NULL)) AS rn
        FROM {tabla}
    ) AS sub
    WHERE rn > 1;
    """
    df_duplicados = pd.read_sql(query_duplicados, engine)
    print("Número de filas duplicadas:", df_duplicados['Num_Filas_Duplicadas'][0])
    
    print("-" * 50)
