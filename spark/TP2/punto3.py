from pyspark import SparkContext
import os
import shutil

# 3) Implemente un script de Spark que permita conocer la cantidad de vehículos (distintos) 
# en movimiento por franja horaria. Un vehículo se considera en movimiento dentro de 
# una franja horario si al menos registra N geolocalizaciones dentro de la propia franja. La 
# duración de la franja horaria y el N son parámetros de la consulta. 

# • ID_Vehiculo
# • Latitud
# • Longitud
# • Timestamp
# • Destino

sc = SparkContext("local[*]", "TP2 - Punto 3")

path_root = "./"
dirInput = path_root + "datasets/input/Viajes/"
dirOutput = path_root + "datasets/output/Viajes/"

DURACION_FRANJA = 3000  # Duración de la franja horaria en segundos
N = 10  # Número mínimo de geolocalizaciones para considerar un vehículo en movimiento

viajes = sc.textFile(dirInput + "*.txt")

def mapViajesFranjas(line):
    parts = line.split("\t")
    id_vehiculo = int(parts[0])
    timestamp = int(parts[3])
    inicio_franja = (timestamp // DURACION_FRANJA) * DURACION_FRANJA
    fin_franja = inicio_franja + DURACION_FRANJA
    return (((inicio_franja, fin_franja), id_vehiculo), 1)

viajesFranjas = viajes.map(mapViajesFranjas)

# Contamos la cantidad de geolocalizaciones por vehículo en cada franja horaria
viajesFranjas = viajesFranjas.reduceByKey(
    lambda acc, value: acc + value
)

# Filtramos los vehículos que tienen al menos N geolocalizaciones en la franja horaria
viajesFranjas = viajesFranjas.filter(lambda x: x[1] >= N)

# Mapeamos cada franja horaria a un contador de vehículos
viajesFranjas = viajesFranjas.map(lambda x: (x[0][0], 1))

# Contamos la cantidad de vehículos por franja horaria
viajesFranjas = viajesFranjas.reduceByKey(
    lambda acc, value: acc + value
)

# Formatear la salida como "inicio_franja fin_franja cantidad_vehiculos"
# Ordenar por franja horaria de manera ascendente
viajesFranjas = viajesFranjas.sortByKey().map(lambda x: f"{x[0][0]}\t{x[0][1]}\t{x[1]}")

output = dirOutput + "viajes_franjas"
if os.path.exists(output):
    shutil.rmtree(output)

# Guardamos el resultado en un solo archivo de salida
viajesFranjas.coalesce(1).saveAsTextFile(output)

sc.stop()