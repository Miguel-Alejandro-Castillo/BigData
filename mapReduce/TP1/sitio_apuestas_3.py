from mre import Job
from .helpers import combiner_marcas, obtener_marcas
root_path = "./"
inputPlatinumDir = root_path + "Apuestas/input/platinum"
inputPremiumDir = root_path + "Apuestas/input/premium"

outputPuntoCDir = root_path + "Apuestas/output/puntoC"

VALUE_X = 2
MARCA_PLATINUM = "PLA"
MARCA_PREMIUM = "PRE"

def fMap(key, value, context, mark):
    X = context["X"]
    campos = value.split("\t")
    idEquipoLocal = int(key)
    apuestasLocal = int(campos[1])
    apuestasVisitante = int(campos[2])

    if (apuestasLocal > (apuestasVisitante * X)):
        context.write(idEquipoLocal, mark)

def fMapPlatinum(key, value, context):
    fMap(key, value, context, MARCA_PLATINUM)
    
def fMapPremium(key, value, context):
    fMap(key, value, context, MARCA_PREMIUM)

def fRed(key, values, context):
    marcas = obtener_marcas(values)
    if marcas == {MARCA_PLATINUM}:
        context.write(key, "") 

job = Job(inputPlatinumDir, outputPuntoCDir, fMapPlatinum, fRed)
job.addInputPath(inputPremiumDir, fMapPremium)
job.setCombiner(combiner_marcas)
job.setParams({ "X": VALUE_X })
success = job.waitForCompletion()
