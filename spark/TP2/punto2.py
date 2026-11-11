from pyspark import SparkContext
import os
import shutil

# 2) Implemente un script de Spark que permita conocer cual es el top 3 de los "tipos" de 
# destinos más visitados. NO interesa contar a los destinos "Otro". 

# • ID_Vehiculo
# • Latitud
# • Longitud
# • Timestamp
# • Destino

sc = SparkContext("local[*]", "TP2 - Punto 2")

path_root = "./"
dirInput = path_root + "datasets/input/Viajes/"
dirOutput = path_root + "datasets/output/Viajes/"

viajes = sc.textFile(dirInput + "*.txt")

def mapViajes(line):
    parts = line.split("\t")
    id_vehiculo = int(parts[0])
    destino = parts[4].strip() # El destino se limpia de espacios en blanco al inicio y al final
    return (destino, id_vehiculo)

viajesDestinos = viajes.map(mapViajes)

# Filtramos los viajes que tienen un destino válido y que no sea "Otro"
viajesDestinos = viajesDestinos.filter(lambda x: x[0] != "" and x[0] != "Otro")

# Contamos la cantidad de viajes por destino
viajesDestino = viajesDestinos.aggregateByKey(
    0,
    lambda acc, x: acc + 1,
    lambda acc1, acc2: acc1 + acc2
)

# Obtenemos el top 3 de destinos más visitados
top3Destinos = viajesDestino.top(3, key=lambda x: x[1])
#top3Destinos = viajesDestino.takeOrdered(3, key=lambda x: -x[1])

# Formateamos el top 3 de destinos para que sea más legible, usando tabulador como separador
top3Destinos = sc.parallelize(top3Destinos).map(lambda x: f"{x[0]}\t{x[1]}")

output = dirOutput + "top3_destinos"
if os.path.exists(output):
    shutil.rmtree(output)

# Guardamos el resultado en un solo archivo de salida
top3Destinos.coalesce(1).saveAsTextFile(output)

sc.stop()