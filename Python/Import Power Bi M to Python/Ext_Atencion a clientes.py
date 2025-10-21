import pandas as pd
import requests
import json
import numpy as np
import os # Importar os para manejo de rutas
from datetime import date, time 

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
API_URL = "https://nominas.grupo-ortiz.site/Controllers/whatsappController.php"
API_PARAMS = {"op": "servicio-cliente"}
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


RUTA_EXPORTACION = r"c:\Users\USUARIO\Downloads\Ext_Atencion a clientes.xlsx"

# Nombres de las columnas originales y sus nuevos nombres
COLUMNS_RENAME_MAP = {
    "content": "Comentario",
    "rate": "Calificación",
    "branch_office": "planta",
    "category": "Categoría",
    "attended_at": "Fecha Atendido",
    "attended_by": "Empleado",
    "order_sale": "Orden de Venta",
    "cliente": "Cliente",
    "phone": "Teléfono",
    "status": "Status",
    "folio": "Folio",
    "clasification": "Clasificación",
}

# Orden final de las columnas
FINAL_COLUMN_ORDER = [
    "Fecha", "planta", "Cliente", "Teléfono", "Empleado", "Categoría", 
    "Clasificación", "Comentario", "Orden de Venta", "Folio", "Status", 
    "Fecha Inicio", "Fecha Atendido", "Hora de Inicio", "Hora de Fin", 
    "Tiempo Atencion Minutos", "IndiceAtencionClientes"
]

# ==============================================================================
# FUNCIÓN DE TRANSFORMACIÓN (M a Python/Pandas)
# ==============================================================================
def transformar_servicio_cliente():
    print("Iniciando extracción de la API...")

    # --- 1. Obtener JSON desde la URL (Source) ---
    try:
        response = requests.get(API_URL, params=API_PARAMS, headers=HEADERS, timeout=30)
        response.raise_for_status() 
        json_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR de conexión: Falló la conexión o la API. {e}")
        return pd.DataFrame()
    except json.JSONDecodeError:
        print("❌ ERROR: La respuesta de la API no es un JSON válido.")
        return pd.DataFrame()

    # --- 2. Acceder a la lista de objetos dentro del campo "data" (Datos) ---
    if "data" not in json_data or not isinstance(json_data["data"], list):
        print("❌ ERROR: La clave 'data' no existe o no contiene una lista de registros.")
        return pd.DataFrame()
        
    Datos = json_data["data"]

    # --- 3. Convertir la lista de registros en una tabla (Tabla) ---
    try:
        df = pd.DataFrame(Datos)
        print(f"✅ Datos extraídos. Registros iniciales: {len(df)}")
    except Exception as e:
        print(f"❌ ERROR: Falló la conversión a DataFrame. {e}")
        return pd.DataFrame()

    # --- 4. Asegurar columnas y Transformaciones iniciales ---
    
    # Asegurar que todas las columnas usadas en M existan
    required_m_cols = ["cliente", "phone", "date", "order_sale", "attended_by", "attended_at", 
                       "category", "branch_office", "rate", "status", "content", "folio", "clasification"]
    for col in required_m_cols:
        if col not in df.columns:
            df[col] = np.nan

    # 5a. Tipo cambiado: Convertir a datetime
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    df["attended_at"] = pd.to_datetime(df["attended_at"], errors='coerce')

    # 5b. Cálculos de tiempo (Hora de inicio, Hora de fin, Tiempo atencion)
    df["Hora de Inicio"] = df["date"].dt.time
    df["Hora de Fin"] = df["attended_at"].dt.time
    
    # Tiempo atencion: Calcula la diferencia y la convierte a minutos
    df["Tiempo Atencion Minutos"] = (df["attended_at"] - df["date"]).dt.total_seconds() / 60

    # 5c. Mes date agregada 
    df["mes date"] = df["date"].dt.month
    
    # 5d. Índice agregado
    df["IndiceAtencionClientes"] = df.index + 1 
    
    # --- 6. Renombrado y Reordenamiento ---
    
    # 6a. Renombrar y crear la columna Fecha
    df = df.rename(columns=COLUMNS_RENAME_MAP)
    
    # Crear la columna "Fecha" 
    # Usamos df["date"] porque el rename de 'date' a 'Fecha Inicio' se hace en la línea siguiente
    df["Fecha"] = df["date"].dt.date 
    
    # Renombrar 'date' a 'Fecha Inicio'
    df = df.rename(columns={"date": "Fecha Inicio"}) 
    
    # 6b. Tipo cambiado2 y Limpieza
    df["Clasificación"] = df["Clasificación"].astype(str)
    df["Tiempo Atencion Minutos"] = pd.to_numeric(df["Tiempo Atencion Minutos"], errors='coerce')
    # Calificación (rate) - Convertir a entero que soporta nulos (Int64.Type)
    df["Calificación"] = pd.to_numeric(df["Calificación"], errors='coerce').astype('Int64')
    
    # 6c. Columnas quitadas
    df = df.drop(columns=["mes date"], errors='ignore')
    
    # 6d. Columnas reordenadas
    df = df.reindex(columns=FINAL_COLUMN_ORDER)

    print("✔️ Transformación completada.")
    return df

# ==============================================================================
# EJECUCIÓN
# ==============================================================================
if __name__ == '__main__':
    df_final = transformar_servicio_cliente()

    if not df_final.empty:
        print("\n================ RESULTADO DE PANDAS ================")
        print(f"Filas resultantes: {len(df_final)}")
        print("\nPrimeras 5 filas:")
        print(df_final.head().to_markdown(index=False))
        print("\nTipos de datos finales:")
        print(df_final.dtypes)
        
        # --- EXPORTACIÓN A EXCEL ---
        try:
            # Exportar a Excel sin el índice de Pandas
            df_final.to_excel(RUTA_EXPORTACION, index=False)
            print(f"\n Exportación completada: Los datos se guardaron en: {RUTA_EXPORTACION}")
        except Exception as e:
            print(f"\n❌ ERROR al exportar a Excel. Asegúrate de que el archivo no esté abierto y la ruta sea válida. {e}")
    else:
        print("\n🛑 El DataFrame final está vacío. No se puede exportar.")