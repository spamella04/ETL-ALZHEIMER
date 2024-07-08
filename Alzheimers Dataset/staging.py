import configparser
import pyodbc
import pandas as pd
import sqlalchemy as db
import logging

# Configurar logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

try:
    # Instancia de la clase ConfigParser
    logging.info('Leyendo el archivo de configuracion')
    config = configparser.ConfigParser()
    config.read('configDatabase.ini')

    server = config['database']['server']
    database = config['database']['database']
    trusted_connection = config['database'].get('trusted_connection', 'no')

    # Autenticación de Windows
    if trusted_connection.lower() == 'yes':
        logging.info('Usando autenticacion de Windows')
        connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    else:
        # Autenticación de SQL Server
        logging.info('Usando autenticacion de SQL Server')
        username = config['database']['username']
        password = config['database']['password']
        connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

    # Conexión SQLAlchemy
    logging.info('Estableciendo conexion a la base de datos')
    engine = db.create_engine(connection_string)
    logging.info('Conexion establecida')

except Exception as e:
    logging.error('Error al crear la conexion: %s', e)
    raise


#Leer archivo sql

try:
    logging.info('Leyendo archivo sql')
    with open('QuerySql/SelectTable.sql', 'r') as file:
        query = file.read()
    logging.info('Archivo sql leido')
except Exception as e:
    logging.error('Error al leer el archivo sql: %s', e)
    raise

# Carga de datos extraido a un DataFrame
try:
    logging.info('Extrayendo datos de la base de datos')
    data = pd.read_sql(query, engine)
    logging.info('Datos extraidos')
except Exception as e: 
    logging.error('Error al extraer datos: %s', e)
    raise 

# Limpieza y Transformación de datos

try:
    logging.info('Iniciando limpieza y transformación de datos')

    # Eliminar columnas duplicadas
    data = data.loc[:, ~data.columns.duplicated()]

 

    data = data.assign(
    Data_Value=data['Data_Value'].fillna(0),
    Low_Confidence_Limit=data['Low_Confidence_Limit'].fillna(0),
    High_Confidence_Limit=data['High_Confidence_Limit'].fillna(0),
    StratificationCategory2=data['StratificationCategory2'].fillna('No Data Available'),
    Stratification2=data['Stratification2'].fillna('No Data Available'),
    AgeGroup=data['AgeGroup'].fillna('No Data Available').str.replace('years', '').str.strip(),
    Geolocation=data['Geolocation'].fillna('No Data Available')
   

    )
    # Separar la columna Geolocation en Latitude y Longitude
    data[['Longitude', 'Latitude']] = data['Geolocation'].str.extract(r'POINT \(([^ ]+) ([^ ]+)\)')
    data['Latitude'] = data['Latitude'].astype(str)
    data['Longitude'] = data['Longitude'].astype(str)

    data = data.assign(
    Latitude=data['Latitude'].str.replace('nan', '0'),
    Longitude=data['Longitude'].str.replace('nan', '0'),
    AgeGroup=data['AgeGroup'].replace('65  or older', '>=65')
    )

    data['RowId'] = data['RowId'].str.split('~')
    data['RowId'] = data.apply(lambda x: '~'.join([x['RowId'][i] for i in range(len(x['RowId'])) if i not in [6, 7]] + [x['AgeGroup'], x['Stratification2']]), axis=1)


    
    # Seleccionar y renombrar columnas para la tabla transformada
    data_transformed = data[['RowId', 'YearStart', 'YearEnd', 'LocationAbbr', 'LocationDesc', 'Class', 'Topic', 'Question', 
                            'Data_Value_Unit', 'DataValueTypeID', 'Data_Value_Type', 'Data_Value', 'Low_Confidence_Limit', 
                            'High_Confidence_Limit', 'AgeGroup', 'StratificationCategory2', 'Stratification2', 
                            'Geolocation', 'Latitude', 'Longitude', 'ClassID', 'TopicID', 'QuestionID', 'LocationID', 
                            'StratificationCategoryID2', 'StratificationID2']]

    

    logging.info('Limpieza y transformación de datos completadas')

except Exception as e:
    logging.error('Error durante la limpieza y transformación de datos: %s', e)
    raise

#Elimnando datos en tabla transformada

try:
    sql_delete_data = 'QuerySql/DeleteData.sql'
    with open(sql_delete_data, 'r') as file:
        queryDeleteData = file.read()
        logging.info('Leyendo archivo sql para eliminar datos en tabla transformada')
except Exception as e:
    logging.error('Error al leer archivo sql para eliminar datos en tabla transformada: %s', e)
    raise

try:
    with engine.connect() as connection:
        trans = connection.begin()
        connection.execute(db.text(queryDeleteData))
        trans.commit()
        logging.info('Datos eliminados en tabla transformada')
except Exception as e:
    logging.error('Error al eliminar datos en tabla transformada: %s', e)
    trans.rollback()
    raise

# Cargar datos limpios y transformados a la tabla AlzheimersDiseaseTransformada en la base de datos

try:
    logging.info('Cargando datos limpios y transformados a la base de datos')
    data_transformed.to_sql('AlzheimersDiseaseTransformada', engine, if_exists='replace', index=False)
    logging.info('Datos cargados a la tabla')
except Exception as e:
    logging.error('Error al cargar datos a la tabla: %s', e)
    raise

