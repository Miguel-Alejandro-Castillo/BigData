def obtener_marcas(values, max_distintas=2):
    marcas = set()
    for value in values:
        marcas.add(value)
        if len(marcas) == max_distintas:
            break
    return marcas
    
def combiner_marcas(key, values, context, max_distintas=2):
    marcas = obtener_marcas(values, max_distintas)
    for marca in marcas:
        context.write(key, marca)