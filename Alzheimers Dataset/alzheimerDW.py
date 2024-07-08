import configparser
import pyodbc
import pandas as pd
import sqlalchemy as db
import logging
import os

# Configurar logging
logging.basicConfig(filename='etlDW.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Leer configuración para Staging
try:
    logging.info('Leyendo configuración para la base de datos fuente')
    config = configparser.ConfigParser()
    config.read('configDatabase.ini')

    staging_server = config['database']['server']
    staging_database = config['database']['database']
    staging_trusted_connection = config['database'].get('trusted_connection', 'no')

    if staging_trusted_connection.lower() == 'yes':
        logging.info('Usando autenticación de Windows para la base de datos fuente')
        staging_connection_string = f'mssql+pyodbc://{staging_server}/{staging_database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    else:
        staging_username = config['database']['username']
        staging_password = config['database']['password']
        staging_connection_string = f'mssql+pyodbc://{staging_username}:{staging_password}@{staging_server}/{staging_database}?driver=ODBC+Driver+17+for+SQL+Server'

    staging_engine = db.create_engine(staging_connection_string)
    logging.info('Conexión a la base de datos fuente establecida')

except Exception as e:
    logging.error('Error al crear la conexión a la base de datos fuente: %s', e)
    raise

# Leer configuración para la base DW
try:
    logging.info('Leyendo configuración para la base de datos destino')
    dw_server = config['databaseDW']['server']
    dw_database = config['databaseDW']['database']
    dw_trusted_connection = config['databaseDW'].get('trusted_connection', 'no')

    if dw_trusted_connection.lower() == 'yes':
        logging.info('Usando autenticación de Windows para la base de datos destino')
        dw_connection_string = f'mssql+pyodbc://{dw_server}/{dw_database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    else:
        dw_username = config['databaseDW']['username']
        dw_password = config['databaseDW']['password']
        dw_connection_string = f'mssql+pyodbc://{dw_username}:{dw_password}@{dw_server}/{dw_database}?driver=ODBC+Driver+17+for+SQL+Server'

    dw_engine = db.create_engine(dw_connection_string)
    logging.info('Conexión a la base de datos destino establecida')

except Exception as e:
    logging.error('Error al crear la conexión a la base de datos destino: %s', e)
    raise


## Leer archivo SQL para las consultas de las tablas dimensionales
try:
    logging.info('Leyendo archivos SQL para las tablas dimensionales')

    with open('QuerySql/DimQuestion.sql', 'r') as file:
        query_dim_question = file.read()

    with open('QuerySql/DimLocation.sql', 'r') as file:
        query_dim_location = file.read()

    with open('QuerySql/DimTopic.sql', 'r') as file:
        query_dim_topic = file.read()

    with open('QuerySql/DimDataValueType.sql', 'r') as file:
        query_dim_data_value_type = file.read()

    with open('QuerySql/DimAgeGroup.sql', 'r') as file:
        query_dim_ageGroup = file.read()

    with open('QuerySql/DimStratification2.sql', 'r') as file:
        query_dim_stratification2 = file.read()

    with open('QuerySql/FactTable.sql', 'r') as file:
        query_fact_table = file.read()

    logging.info('Archivos SQL leídos correctamente')

except Exception as e:
    logging.error('Error al leer los archivos SQL: %s', e)
    raise


# Ejecutar consultas y cargar datos a DataFrames
try:
    logging.info('Ejecutando consultas y cargando datos a DataFrames')

    dim_question = pd.read_sql(query_dim_question, staging_engine)
    dim_location = pd.read_sql(query_dim_location, staging_engine)
    dim_topic = pd.read_sql(query_dim_topic, staging_engine)
    dim_data_value_type = pd.read_sql(query_dim_data_value_type, staging_engine)
    dim_ageGroup = pd.read_sql(query_dim_ageGroup, staging_engine)
    dim_stratification2 = pd.read_sql(query_dim_stratification2, staging_engine)
    fact_alzheimer = pd.read_sql(query_fact_table, staging_engine)
  

    logging.info('Datos cargados a DataFrames correctamente')

except Exception as e:
    logging.error('Error al cargar datos a DataFrames: %s', e)
    raise

#Elimnando datos en base de datos dimensional

try:
    sql_delete_dimensional = 'QuerySql/DeleteDimensional.sql'
    with open(sql_delete_dimensional, 'r') as file:
        queryDeleteData = file.read()
        logging.info('Leyendo archivo sql para eliminar datos en tablas dimensionales')
except Exception as e:
    logging.error('Error al leer archivo sql para eliminar datos en tablas dimensionales: %s', e)
    raise

try:
    with dw_engine.connect() as connection:
        trans = connection.begin()
        connection.execute(db.text(queryDeleteData))
        trans.commit()
        logging.info('Datos eliminados en tablas dimensionales')
except Exception as e:
    logging.error('Error al eliminar datos en tablas dimensionales: %s', e)
    trans.rollback()
    raise
# Insertar datos en las tablas dimensionales de la base de datos destino
try:
    logging.info('Insertando datos en las tablas dimensionales de la base de datos destino')

    dim_question.to_sql('DimQuestion', dw_engine, if_exists='append', index=False)
    dim_location.to_sql('DimLocation', dw_engine, if_exists='append', index=False)
    dim_topic.to_sql('DimTopic', dw_engine, if_exists='append', index=False)
    dim_data_value_type.to_sql('DimDataValueType', dw_engine, if_exists='append', index=False)
    dim_ageGroup.to_sql('DimAgeGroup', dw_engine, if_exists='append', index=False)
    dim_stratification2.to_sql('DimStratification2', dw_engine, if_exists='append', index=False)
    fact_alzheimer.to_sql('FactAlzheimersDisease', dw_engine, if_exists='append', index=False)

    logging.info('Datos insertados en las tablas dimensionales correctamente')

except Exception as e:
    logging.error('Error al insertar datos en las tablas dimensionales: %s', e)
    raise