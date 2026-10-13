from .helpers import obtener_marcas, combiner_marcas
from mre import Job

root_path = "./"
inputPlatinumDir = root_path + "Apuestas/input/platinum"
inputEstandarDir = root_path + "Apuestas/input/estandar"

outputPuntoBDir = root_path + "Apuestas/output/puntoB"

MARCA_PLATINUM = "PLA"
MARCA_ESTANDAR = "EST"

def fMap(key, value, context, marca):
    campos = value.split("\t")
    idEquipoVisitante = int(campos[0])
    apuestasLocal = int(campos[1])
    apuestasVisitante = int(campos[2])

    if apuestasVisitante > apuestasLocal:
        context.write(idEquipoVisitante, marca)

def fMapP(key, value, context):
    fMap(key, value, context, MARCA_PLATINUM)

def fMapE(key, value, context):
    fMap(key, value, context, MARCA_ESTANDAR)

def fRed(key, values, context):
    marcas = obtener_marcas(values)
    if len(marcas) == 2:
        context.write(key, "")
    
job = Job(inputPlatinumDir, outputPuntoBDir, fMapP, fRed)
job.addInputPath(inputEstandarDir, fMapE)
job.setCombiner(combiner_marcas)
success = job.waitForCompletion()
