import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import requests
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime,timedelta
import redshift_connector
import smtplib
from email.mime.text import MIMEText

from pathlib import Path
import psycopg2
from airflow import DAG
from airflow.models import variable
from sqlalchemy import create_engine,text
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago

import sys
import os
#proyecto_de_path = os.path.abspath(os.path.join(os.path.dirname(sys.path[0]), "Spotify_ProjectDE"))
#sys.path.insert(0, proyecto_de_path)
#from functions import consultar_APISpotify


# Configuración de la API de Spotify
def configurar_keys():
    client_id = os.getenv('CLIENT_ID')              # <- variable de entorno
    client_secret = os.getenv('CLIENT_SECRET')      # <- variable de entorno
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return spotify

#configuración de los datos de la url de coneccion a la db
def configurar_keys_db():
    usuario = os.getenv('USUARIO')
    password = os.getenv('PASSWORD')
    nombre_base = os.getenv('NOMBRE_BASE')
    host = os.getenv('HOST')
    puerto = os.getenv('PUERTO')
    return usuario,password,nombre_base,host,puerto

# Obtener datos de Spotify
def get_spotify_data(spotify):
    songs = []
    songs_results = spotify.search(q='track:', type='track', limit=50)  # Limito a 50 canciones
    songs.extend(songs_results['tracks']['items'])
    return songs

def obtener_datos(songs_data,spotify):
    canciones = []
    fact_table = []
    artistas= []
    tiempo = []
    fecha_hora_actual = datetime.now()
    fecha_actual = fecha_hora_actual.date()
    mes_actual = fecha_hora_actual.month
    anio_actual = fecha_hora_actual.year
    dia_actual = fecha_hora_actual.day
    for j in songs_data:
        for song in j:
             id = song['id']
             name = song['name']
             duration = song['duration_ms']
             release_year = song['album']['release_date'][:4]
             popularity = song['popularity']
             artistas_song = song['artists']
             for a in artistas_song:
                 id_artista = a['id']
                 artist = spotify.artist(id_artista) #con el id del artista recupero su info
                 nombre_artista = a['name']
                 seguidores_artista = artist['followers']['total']
                 artistas.append({'cod_artista':id_artista,'nombre': nombre_artista})
                 fact_table.append({'cod_cancion':id,'cod_artista':id_artista,'fecha':fecha_actual,'popularidad': popularity,'nro_seguidores':seguidores_artista})
             canciones.append({'cod_cancion':id, 'nombre': name,'duracion': duration,'anio_publicacion': release_year})
             tiempo.append({'cod_tiempo':fecha_actual,'anio':anio_actual,'mes':mes_actual,'dia':dia_actual})
    return canciones,artistas,fact_table,tiempo

def eliminar_duplicados(df_canciones,df_artistas,df,df_fechas):
    df_canciones = df_canciones.drop_duplicates()
    df_artistas = df_artistas.drop_duplicates()
    df = df.drop_duplicates()
    df_fechas = df_fechas.drop_duplicates()
    return df_canciones,df_artistas,df,df_fechas


def carga_database(df_canciones,df_artistas,df,df_fechas):
    #obtengo parametros para armar la url para conectar a la bd
    usuario,password,nombre_base,host,puerto = configurar_keys_db()
    #armo url para conectar a la base:
    """conn = redshift_connector.connect(
     host=host,
     database=nombre_base,
     port=puerto,
     user=usuario,
     password= password
    )"""
    url = 'postgresql://' + usuario + ':' + password + '@' + host + ':' + str(puerto) + '/' + nombre_base
    #conecto a la base de datos:
    conn = create_engine(url)
    df.to_sql(name='facttable',con=conn,schema='ayedemaria_coderhouse',if_exists='append',index=False)
    #el tiempo no se repite porque se corre una vez al día por lo tanto no es necesario eliminar repetidos:
    df_fechas.to_sql(name='dimtiempo',con=conn,schema='ayedemaria_coderhouse',if_exists='append',index=False)
    #obtengo artistas y canciones ya guardados en db:
    result_artistas = pd.read_sql_query('SELECT cod_artista FROM ayedemaria_coderhouse.dimartistas',con = conn)
    result_canciones = pd.read_sql_query('SELECT cod_cancion FROM ayedemaria_coderhouse.dimcanciones',con = conn)
    #me quedo con los artistas que no se repitan con los ya cargadas en la base de datos:
    merged_artistas = df_artistas.merge(result_artistas, how='left', indicator=True)
    filtered_artistas = merged_artistas[merged_artistas['_merge'] == 'left_only'].drop('_merge', axis=1)
    #guardo en db solo los artistas nuevos:
    filtered_artistas.to_sql(name='dimartistas',con=conn,schema='ayedemaria_coderhouse',if_exists='append',index=False)
    #me quedo con las canciones que no se repitan con las ya cargadas en la base de datos:
    merged_canciones = df_canciones.merge(result_canciones, how='left', indicator=True)
    filtered_canciones = merged_canciones[merged_canciones['_merge'] == 'left_only'].drop('_merge', axis=1)
    #guardo en db solo las canciones nuevas:
    filtered_canciones.to_sql(name='dimcanciones',con=conn,schema='ayedemaria_coderhouse',if_exists='append',index=False)
    print('artistas sin repetir ---------')
    print(filtered_artistas)
    print('canciones sin repetir ---------')
    print(filtered_canciones)
    return None

def main():
    load_dotenv() # carga las variables de entorno desde el archivo .env.

    spotify = configurar_keys()

    songs_data = []
    for i in range(3):
        songs_data.append(get_spotify_data(spotify)) #tengo 3 listas de 50 canciones cada una

    canciones,artistas,fact_table,tiempo = obtener_datos(songs_data,spotify)

    df_canciones = pd.DataFrame(canciones)
    df_artistas = pd.DataFrame(artistas)
    df = pd.DataFrame(fact_table)
    df_fechas = pd.DataFrame(tiempo)

    df_canciones,df_artistas,df,df_fechas = eliminar_duplicados(df_canciones,df_artistas,df,df_fechas)

    carga_database(df_canciones,df_artistas,df,df_fechas)



def send_email(subject,body,sender,recipients,password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Mensaje enviado!")

# argumentos por defecto para el DAG
default_args = {
    'owner': 'AyelenD',
    'start_date': datetime(2023,8,22),
    'retries':3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'Spotify_ETL',
    description='Agrega data de Spotify de forma diaria',
    schedule_interval='0 12 * * *',
    catchup=False,
    default_args=default_args) as dag:

    def ejecutar_main(**kwargs):
        main()

    def enviar_mail(**kwargs):
        subject = "ALERTA - Carga de datos de Spotify"
        body = "Se ha cargando correctamente la información obtenida desde la API de spotify"
        envia = os.getenv('SENDER')
        sender = f'{envia}'
        recipients = ["ayedemaria+1@gmail.com"]
        password_envia = os.getenv('SENDER_PASSWORD')
        password = f'{password_envia}'

        send_email(subject,body,sender,recipients,password)

    # Tareas
    obtener_datos_APISpotify = PythonOperator(
        task_id='ETL_Spotify',
        python_callable=ejecutar_main,
        #op_args=["{{ ds }} {{ execution_date.hour }}"],
        provide_context=True,
        dag=dag
    )
    enviar_mail = PythonOperator(
        task_id='envio_mail',
        python_callable=enviar_mail,
        dag=dag
    )
    # Definicion orden de tareas
    obtener_datos_APISpotify >> enviar_mail
