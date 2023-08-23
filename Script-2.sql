CREATE TABLE IF NOT EXISTS DimCanciones (
cod_cancion INT IDENTITY(1,1) PRIMARY KEY,
id_spotify varchar(250) NOT NULL,
nombre varchar(250) NOT NULL,
duracion int NOT NULL,
a�o_publicacion INT);

CREATE TABLE IF NOT EXISTS DimArtistas
(cod_artista INT IDENTITY(1,1) PRIMARY KEY,
nombre varchar(250) NOT NULL);

CREATE TABLE IF NOT EXISTS DimTiempo
(cod_tiempo date PRIMARY KEY,
anio int NOT NULL,
mes int NOT NULL,
dia int NOT NULL);

CREATE TABLE IF NOT EXISTS FactTable
(cod_tiempo date NOT NULL,
cod_cancion INT NOT NULL,
cod_artista INT NOT NULL,
popularidad INT NOT NULL,
nro_seguidores INT NOT NULL);
