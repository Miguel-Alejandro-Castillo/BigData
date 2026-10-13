from mre import Job
from .helpers import obtener_marcas, combiner_marcas

root_path = "./"
inputPlatinumDir = root_path + "Apuestas/input/platinum"
inputPremiumDir = root_path + "Apuestas/input/premium"
inputEstandarDir = root_path + "Apuestas/input/estandar"

outputPuntoDDir = root_path + "Apuestas/output/puntoD"

MARCA_PLATINUM = "PLA"
MARCA_PREMIUM = "PRE"
MARCA_ESTANDAR = "EST"

# A△B=(A−B)∪(B−A) diferencia simétrica

def fMapPlatinum(key, value, context):
    idEquipoLocal = int(key)
    context.write(idEquipoLocal, MARCA_PLATINUM)
    
def fMapPremium(key, value, context):
    campos = value.split("\t")
    idEquipoVisitante = int(campos[0])
    context.write(idEquipoVisitante, MARCA_PREMIUM)

def fMapEstandar(key, value, context):
    campos = value.split("\t")
    idEquipoLocal = int(key)
    idEquipoVisitante = int(campos[0])
    context.write(idEquipoLocal, MARCA_ESTANDAR)
    context.write(idEquipoVisitante, MARCA_ESTANDAR)

def fRed(key, values, context):
    marcas = obtener_marcas(values)
    # Si solo hay una marca asociada a la clave, se escribe la clave con un valor vacío.
    if len(marcas) == 1: # diferencia simetrica
        context.write(key, "")

job = Job(inputPlatinumDir, outputPuntoDDir, fMapPlatinum, fRed)
job.addInputPath(inputPremiumDir, fMapPremium)
job.addInputPath(inputEstandarDir, fMapEstandar)
job.setCombiner(combiner_marcas)
success = job.waitForCompletion()
