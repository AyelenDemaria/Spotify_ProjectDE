import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import requests
from dotenv import load_dotenv
import os

load_dotenv() # carga las variables de entorno desde el archivo .env.

iteracion = 3 #variable para iterar la consultas a la api

# Configuración de la API de Spotify
client_id = os.getenv('CLIENT_ID')              # <- variable de entorno
client_secret = os.getenv('CLIENT_SECRET')      # <- variable de entorno
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


# Obtener datos de Spotify
def get_spotify_data():
    songs = []
    songs_results = spotify.search(q='track:', type='track', limit=50)  # Limito a 50 canciones
    songs.extend(songs_results['tracks']['items'])
    return songs

# Obtener los datos de Spotify
for i in range(iteracion):
    songs_data = get_spotify_data()

    # Mostrar los datos por pantalla
    for song in songs_data:
    	print('  ID:', song['id'])
    	print('  Titulo:', song['name'])
    	print('  Duración:', song['duration_ms'])
    	print('  Año de publicacion:', song['album']['release_date'][:4])
    	print('  Popularidad:', song['popularity'])
    	artistas = song['artists'] #obtengo artistas de la cancion
    	print('artistas:')
    	for i in artistas:
    		id = i['id']
    		artist = spotify.artist(id) #con el id del artista recupero su info
    		print('  nombre:', i['name'], ' // ', 'Seguidores:', artist['followers']['total'])
    	print('----------------------------')
