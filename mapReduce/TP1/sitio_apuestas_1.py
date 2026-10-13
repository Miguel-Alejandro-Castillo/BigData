from mre import Job

root_path = "./"
inputPremiumDir = root_path + "Apuestas/input/premium"
inputEstandarDir = root_path + "Apuestas/input/estandar"

outputPuntoADir = root_path + "Apuestas/output/puntoA"

VALUE_X = 1000

def fMap(key, value, context):
    limite = context["X"]
    campos = value.split("\t")
    
    idEquipoLocal = int(key)
    idEquipoVisitante = int(campos[0])
    apuestasLocal = int(campos[1])
    apuestasVisitante = int(campos[2])

    if apuestasLocal > limite:
        context.write(idEquipoLocal, "")

    if apuestasVisitante > limite:
        context.write(idEquipoVisitante, "")

def fRed(key, values, context):
    context.write(key, "")

job = Job(inputPremiumDir, outputPuntoADir, fMap, fRed)
job.addInputPath(inputEstandarDir, fMap)
job.setParams({ "X": VALUE_X })
job.setCombiner(fRed)
success = job.waitForCompletion()