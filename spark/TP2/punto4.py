from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# 4) Implemente un script de Spark que permita conocer la velocidad promedio de cada 
# vehículo: el promedio de las velocidades de todos sus viajes. 
# La velocidad de un viaje se calcula como el tiempo del viaje (timestamp de fin – 
# timestamp de inicio) dividido la cantidad de cuadras del viaje.
# Definimos el esquema de las 5 columnas del dataset

path_root = "./"
dirInput = path_root + "datasets/input/Viajes/"
dirOutput = path_root + "datasets/output/Viajes/"

spark = (
    SparkSession.builder
    .appName("TP2 - Punto 4")
    .master("local[*]")
    .getOrCreate()
)

ID_VEHICULO = "ID_Vehiculo"
LATITUD = "Latitud"
LONGITUD = "Longitud"
TIMESTAMP = "Timestamp"
DESTINO = "Destino"
ID_VIAJE = "ID_Viaje"

schema = StructType([
    StructField(ID_VEHICULO, IntegerType(), False),
    StructField(LATITUD, DoubleType(), False),
    StructField(LONGITUD, DoubleType(), False),
    StructField(TIMESTAMP, IntegerType(), False),
    StructField(DESTINO, StringType(), True)
])

# ============================================================
# 1. CARGAR Y NORMALIZAR
# ============================================================

df = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .schema(schema)
    .csv(dirInput)
)

df = df.withColumn(
    DESTINO,
    F.when(
        F.col(DESTINO).isNull()
        | (F.trim(F.col(DESTINO)) == ""),
        F.lit(None)
    ).otherwise(
       F.trim(F.col(DESTINO))
    )
)

# ============================================================
# 2. GENERAR ID EN LOS DESTINOS
# ============================================================

df_con_viajes = df.withColumn(
    ID_VIAJE,
    F.when(
        F.col(DESTINO).isNotNull(),
        F.concat_ws(
            "_",
            F.col(ID_VEHICULO),
            F.col(TIMESTAMP)
        )
    )
)

# ============================================================
# 3. PROPAGAR EL DESTINO POSTERIOR MÁS PRÓXIMO
# ============================================================

# Se asume que para un mismo vehículo cada Timestamp identifica una única medición de geolocalización.

ventana_futura = (
    Window
    .partitionBy(ID_VEHICULO)
    .orderBy(TIMESTAMP)
    .rowsBetween(
        Window.currentRow,
        Window.unboundedFollowing
    )
)

df_con_id_propagado = df_con_viajes.withColumn(
    ID_VIAJE,
    F.first(
        ID_VIAJE,
        ignorenulls=True
    ).over(ventana_futura)
)

# Registros posteriores al último destino no forman un viaje completo.
df_con_id_propagado = df_con_id_propagado.filter(
    F.col(ID_VIAJE).isNotNull()
)

# ============================================================
# 4. DISTANCIA MANHATTAN POR TRAMO
# ============================================================

# La cantidad de cuadras se calcula como la suma de las
# distancias Manhattan entre geolocalizaciones consecutivas
# pertenecientes al mismo viaje.

ventana_viaje_ordenada = (
    Window
    .partitionBy(ID_VEHICULO, ID_VIAJE)
    .orderBy(TIMESTAMP)
)

df_con_desplazamiento = (
    df_con_id_propagado
    .withColumn(
        "Lat_prev",
        F.lag(LATITUD).over(ventana_viaje_ordenada)
    )
    .withColumn(
        "Lon_prev",
        F.lag(LONGITUD).over(ventana_viaje_ordenada)
    )
    .withColumn(
        "distancia_manhattan_tramo",
        F.when(
            F.col("Lat_prev").isNotNull()
            & F.col("Lon_prev").isNotNull(),
            F.abs(
                F.col(LATITUD) - F.col("Lat_prev")
            )
            +
            F.abs(
                F.col(LONGITUD) - F.col("Lon_prev")
            )
        ).otherwise(F.lit(0.0))
    )
)

# ============================================================
# 5. MÉTRICAS POR VIAJE
# ============================================================

# Se utiliza la fórmula indicada literalmente por la consigna:
# velocidad_viaje = duracion_viaje / cantidad_cuadras

df_metricas_viaje = (
    df_con_desplazamiento
    .groupBy(ID_VEHICULO, ID_VIAJE)
    .agg(
        F.min(TIMESTAMP).alias("timestamp_inicio"),
        F.max(TIMESTAMP).alias("timestamp_fin"),
        F.sum("distancia_manhattan_tramo").alias(
            "total_cuadras_manhattan"
        )
    )
    .withColumn(
        "duracion_viaje",
        F.col("timestamp_fin")
        - F.col("timestamp_inicio")
    )
    .filter(
        F.col("total_cuadras_manhattan") > 0
    )
    .withColumn(
        "velocidad_viaje",
        F.col("duracion_viaje")
        / F.col("total_cuadras_manhattan")
    )
)

# ============================================================
# 6. PROMEDIO POR VEHÍCULO
# ============================================================

df_resultado_final = (
    df_metricas_viaje
    .groupBy(ID_VEHICULO)
    .agg(
        F.avg("velocidad_viaje").alias(
            "Velocidad_promedio"
        )
    )
    .orderBy(ID_VEHICULO)
)

df_resultado_final.show(
    truncate=False
)

# ============================================================
# 7. GUARDAR
# ============================================================

(
    df_resultado_final.write
    .mode("overwrite")
    .option("header", "false")
    .option("sep", "\t")
    .csv(dirOutput + "velocidades_promedio")
)

spark.stop()