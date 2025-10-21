import requests
import pandas as pd
from pandas import json_normalize # Importamos la función de aplanamiento

# 1. Definir la URL de la API (Ejemplo de API pública, aunque la estructura varía)
# Nota: La estructura del JSON afecta cómo se usa json_normalize. 
# Este ejemplo asume que la respuesta principal es una lista de objetos.
URL_API = 'https://kpis.grupo-ortiz.site/Controllers/apiController.php?op=api'

# 2. Realizar la solicitud HTTP GET
try:
    response = requests.get(URL_API)
    response.raise_for_status()  # Lanza una excepción para códigos de error (4xx o 5xx)
    
    # 3. Obtener los datos JSON como una lista/diccionario de Python
    data_json = response.json()
    
except requests.exceptions.RequestException as e:
    print(f"Error al conectar con la API: {e}")
    # Retorna un DataFrame vacío en caso de error para evitar fallos
    df = pd.DataFrame() 

# 4. Normalizar el JSON a un DataFrame plano
# Si el JSON es una lista de objetos simple:
df_final = json_normalize(
    data_json, 
    # record_path=None si la raíz es la lista que queremos normalizar
    
    # meta=[] Si el JSON tiene campos anidados que queremos aplanar:
    # Por ejemplo, si los datos de la URL anterior tuvieran un objeto 'address', 
    # y este a su vez tuviera 'street' y 'city', json_normalize los aplana.
    # Usamos 'sep' para definir cómo se nombran las columnas anidadas (ej: address_street)
    sep='_' 
)

# 5. Opcional: limpiar y seleccionar columnas



# 6. Mostrar el DataFrame resultante (En un entorno de Power BI,
#    la última línea 'print' es la que devuelve el DataFrame a Power Query)
print(df_final)