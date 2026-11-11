from pyspark import SparkContext
import os
import shutil

# 1) Implemente un script de pySpark que permita conocer cuántos viajes realizó cada 
# vehículo.  Recordar que un viaje es una serie de coordenadas que finalizan en un destino 
# determinado.

# • ID_Vehiculo 
# • Latitud 
# • Longitud 
# • Timestamp 
# • Destino 

sc = SparkContext("local[*]", "TP2 - Punto 1")

path_root = "./"
dirInput = path_root + "datasets/input/Viajes/"
dirOutput = path_root + "datasets/output/Viajes/"

viajes = sc.textFile(dirInput + "*.txt")

def mapViajes(line):
    parts = line.split("\t")
    id_vehiculo = int(parts[0])
    destino = parts[4].strip() # El destino se limpia de espacios en blanco al inicio y al final
    return (id_vehiculo, destino)

viajes = viajes.map(mapViajes)

# Filtramos los viajes que tienen un destino válido (no vacío)
viajes = viajes.filter(lambda x: x[1] != "")

# Contamos la cantidad de viajes por vehículo
viajesPorVehiculo = viajes.aggregateByKey(
    0,
    lambda acc, x: acc + 1,
    lambda acc1, acc2: acc1 + acc2
)

# Formateamos la salida para que sea más legible, usar tabulador como separador
viajesPorVehiculo = viajesPorVehiculo.map(lambda x: f"{x[0]}\t{x[1]}")

output = dirOutput + "vehiculos_viajes"
if os.path.exists(output):
    shutil.rmtree(output)

viajesPorVehiculo.saveAsTextFile(output)

sc.stop()