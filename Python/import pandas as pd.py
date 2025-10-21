import pandas as pd
import requests
from datetime import datetime

# Leer el parámetro pasado desde Power BI
fecha_desde = FechaDesde['FechaDesde'][0]  # tipo datetime
fecha_desde_iso = fecha_desde.isoformat()

# Construir URL de la API con parámetro (suponiendo que la API lo acepta)
url = f"https://api.ejemplo.com/ventas?desde={fecha_desde_iso}"

# Llamar a la API
response = requests.get(url)

# Verificar respuesta
if response.status_code == 200:
    data = response.json()  # Asume que devuelve una lista de objetos/diccionarios
    df = pd.DataFrame(data)
    
    # Asegurar que la columna de fecha está en formato datetime
    df['fecha'] = pd.to_datetime(df['fecha'])
else:
    df = pd.DataFrame()  # Vacío si hay error


import pandas as pd
from datetime import datetime, timedelta

# Leer los datos (puede ser desde CSV, SQL, etc.)
df = pd.read_csv("C:/ruta/datos_ventas.csv", parse_dates=['fecha']) 

# Filtrar últimos 30 días
hoy = datetime.now()
hace_30_dias = hoy - timedelta(days=30)

df_filtrado = df[df['fecha'] >= hace_30_dias]


#########################################################################################################################
import pandas as pd
import requests
from datetime import datetime, timedelta

# --- 1. HISTÓRICO: esta tabla debe existir en el modelo
df_hist = datos_historicos  # <-- esta tabla ya está en el modelo de Power BI
df_hist['fecha'] = pd.to_datetime(df_hist['fecha'])

# --- 2. Obtener fecha máxima cargada
if df_hist.empty:
    # Si no hay datos, pedir los últimos 7 días como base inicial
    fecha_inicio = datetime.now() - timedelta(days=7)
else:
    fecha_inicio = df_hist['fecha'].max()

# --- 3. Llamar a la API con la fecha como parámetro
url_base = "https://api.tudominio.com/datos"
params = {
    "desde": fecha_inicio.isoformat()
}

response = requests.get(url_base, params=params)

# --- 4. Validar respuesta
if response.status_code == 200:
    nuevos = pd.DataFrame(response.json())
    nuevos['fecha'] = pd.to_datetime(nuevos['fecha'])

    # --- 5. Filtrar solo datos más nuevos (por si API trae repetidos)
    nuevos_filtrados = nuevos[nuevos['fecha'] > fecha_inicio]

    # --- 6. Unir histórico + nuevos
    df_final = pd.concat([df_hist, nuevos_filtrados], ignore_index=True)
else:
    # Si falla, simplemente devuelve el histórico sin nuevos datos
    df_final = df_hist
