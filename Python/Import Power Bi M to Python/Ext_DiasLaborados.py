import pandas as pd
import requests
from pandas import json_normalize
import os
from datetime import date 

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
API_URL = "https://kpis.grupo-ortiz.site/Controllers/apiController.php?op=api"
# Columnas que actúan como identificadores en la tabla final
COLUMNAS_IDENTIFICADORAS_UNPIVOT = ["date", "planta", "SEGMENTO"]

# RUTAS DE ARCHIVOS
RUTA_CATALOGO = r"C:\Users\USUARIO\Downloads\Cat_DiasLaborables.xlsx"
RUTA_EXPORTACION = r"C:\Users\USUARIO\Downloads\Reporte_DiasLaborales.xlsx" # Exportación a Excel

# ==============================================================================
# TABLA DE CATÁLOGO (Carga desde Excel)
# ==============================================================================
def cargar_tabla_catalogo():
    """Carga la tabla de catálogo desde el archivo Excel en RUTA_CATALOGO."""
    print(f"Intentando cargar catálogo desde: {RUTA_CATALOGO}")
    try:
        df = pd.read_excel(RUTA_CATALOGO)
        
        if 'DIAS LABORABLES' in df.columns:
            df['DIAS LABORABLES'] = df['DIAS LABORABLES'].astype(str).str.strip().str.upper()
        
        if 'CONCEPTO' not in df.columns:
            print("❌ ERROR: La columna 'CONCEPTO' no se encontró en el archivo Excel.")
            return pd.DataFrame({'DIAS LABORABLES': [], 'CONCEPTO': []})
        
        print(f"✅ Catálogo de días laborales cargado correctamente.")
        return df
        
    except FileNotFoundError:
        print(f"❌ ERROR: Archivo de catálogo no encontrado en la ruta: {RUTA_CATALOGO}")
        return pd.DataFrame({'DIAS LABORABLES': [], 'CONCEPTO': []})
    except Exception as e:
        print(f"❌ ERROR al cargar el catálogo de Excel. Verifica el formato de la hoja. {e}")
        return pd.DataFrame({'DIAS LABORABLES': [], 'CONCEPTO': []})


def transformar_api_a_reporte():
    print("Iniciando extracción y transformación de la API...")

    # --- PASO 1: Extracción de la API (Origen) ---
    try:
        response = requests.get(API_URL, headers={"Accept": "application/json"}, timeout=15)
        response.raise_for_status()
        json_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Falló la conexión o la API. {e}")
        return pd.DataFrame()

    # ==========================================================================
    # --- PASO 2: Aplanamiento DINÁMICO (Simula M: ConvertidoEnTabla/ExpandidoLista/ExpandidoRegistro) ---
    # ==========================================================================
    data_list = []
    
    if isinstance(json_data, list):
        data_list = json_data
    elif isinstance(json_data, dict):
        # Intenta encontrar la lista de registros más relevante (la que contenía los datos)
        for key, value in json_data.items():
            if isinstance(value, list) and all(isinstance(i, dict) for i in value):
                data_list = value
                break
        
        if not data_list:
            # Si no hay lista, trata el diccionario raíz como un solo registro
            data_list = [json_data]
             
    if not data_list:
        print("❌ ERROR: No se encontró una lista de registros válida en el JSON de la API.")
        return pd.DataFrame()

    try:
        # Aplanar la lista de registros (esto crea columnas como 'GENERAL_date', 'GENERAL_DIAS_...')
        df_registros_general = json_normalize(data_list, sep='_') 
        print("✅ Estructura JSON aplanada dinámicamente.")
    except Exception as e:
        print(f"❌ ERROR: Falló el aplanamiento de los registros. {e}")
        return pd.DataFrame()
            
    # ==========================================================================
    # --- PASO 3: Filtrado ESTRICTO de columnas (Simula M: ListaFiltradaCamposGeneral) ---
    # ==========================================================================
    
    cols_a_mantener_fijos = COLUMNAS_IDENTIFICADORAS_UNPIVOT
    cols_a_mantener_prefix = 'GENERAL_' 
    
    # 1. Definir las columnas necesarias
    cols_requeridas = [
        col for col in df_registros_general.columns
        if col in cols_a_mantener_fijos or 
           col.startswith('DIAS') or 
           col.startswith(cols_a_mantener_prefix)
    ]
    
    df_filtrado = df_registros_general[cols_requeridas].copy()
    
    # 2. Renombrar columnas si fueron prefijadas (e.g., 'GENERAL_date' -> 'date')
    rename_map = {}
    for col in df_filtrado.columns:
        if col.startswith(cols_a_mantener_prefix):
            new_col = col.replace(cols_a_mantener_prefix, '')
            # Solo renombramos si el resultado es una columna que nos interesa (date, planta, SEGMENTO o DIAS_*)
            if new_col in cols_a_mantener_fijos or new_col.startswith('DIAS'):
                rename_map[col] = new_col
                
    df_filtrado.rename(columns=rename_map, inplace=True)
    
    # 3. Filtrar de nuevo para las columnas finales requeridas
    required_cols = cols_a_mantener_fijos + [col for col in df_filtrado.columns if col.startswith('DIAS')]
    required_cols = list(set([c for c in required_cols if c in df_filtrado.columns]))
    df_filtrado = df_filtrado[required_cols]

    if not any(col.startswith('DIAS') for col in df_filtrado.columns):
        print("❌ ERROR: No se encontraron las columnas 'DIAS_...' después del aplanamiento y filtrado.")
        return pd.DataFrame()
        
    
    # ==========================================================================
    # --- PASO 4: Anulación de dinamización (Unpivot) ---
    # ==========================================================================
    cols_identificadoras_unpivot = [col for col in COLUMNAS_IDENTIFICADORAS_UNPIVOT if col in df_filtrado.columns]
    
    # CRÍTICO: Solo dinamizar las columnas que empiecen con 'DIAS'
    cols_a_dinamizar = [col for col in df_filtrado.columns if col.startswith('DIAS')]

    df_unpivot = pd.melt(
        df_filtrado,
        id_vars=cols_identificadoras_unpivot,
        value_vars=cols_a_dinamizar, # Solo DIAS_* entran aquí
        var_name="Concepto_Dias",
        value_name="Dias_Laborados"
    )
    
    # --- PASO 5: Join con Catálogo ---
    df_catalogo = cargar_tabla_catalogo()
    
    if df_catalogo.empty:
        print("🛑 Detenido: No se puede realizar el Join sin el catálogo.")
        return pd.DataFrame()

    df_unpivot['Concepto_Dias'] = df_unpivot['Concepto_Dias'].str.upper()

    # El Inner Join asegura que solo quedan los conceptos de días que tienen coincidencia en el catálogo
    df_join = pd.merge(
        df_unpivot,
        df_catalogo,
        left_on='Concepto_Dias',
        right_on='DIAS LABORABLES',
        how='inner'
    )

    # Renombrar 'CONCEPTO' y limpiar columnas
    df_join = df_join.rename(columns={'CONCEPTO': 'Conceptos_DiasLaborados'})
    df_join = df_join.drop(columns=["SEGMENTO", "Concepto_Dias", "DIAS LABORABLES"], errors='ignore')

    # --- PASO 6: Limpieza y Tipado Final ---
    
    columnas_finales = ["date", "planta", "Conceptos_DiasLaborados", "Dias_Laborados"]
    df_resultado = df_join.reindex(columns=columnas_finales)

    df_resultado["date"] = pd.to_datetime(df_resultado["date"], errors='coerce').dt.date
    df_resultado["planta"] = df_resultado["planta"].astype(str)
    df_resultado["Conceptos_DiasLaborados"] = df_resultado["Conceptos_DiasLaborados"].astype(str)
    # Int64.Type en M se traduce a Int64 en Pandas (soporta NaN)
    df_resultado["Dias_Laborados"] = pd.to_numeric(df_resultado["Dias_Laborados"], errors='coerce').astype('Int64')

    df_resultado = df_resultado.drop_duplicates().reset_index(drop=True)

    print("✔️ Transformación de API a Reporte completada.")
    return df_resultado

# ==============================================================================
# EJECUCIÓN
# ==============================================================================
if __name__ == '__main__':
    df_final = transformar_api_a_reporte()

    if not df_final.empty:
        print("\n================ RESULTADO FINAL DE LA EXTRACCION ================")
        print(f"Filas resultantes: {len(df_final)}")
        
        print("Primeras 5 filas:")
        print(df_final.head().to_markdown(index=False))
        print("\nTipos de datos finales:")
        print(df_final.dtypes)
        
        # --- EXPORTACIÓN A EXCEL ---
        try:
            df_final.to_excel(RUTA_EXPORTACION, index=False)
            print(f"\n Exportación completada: Los datos se guardaron en: {RUTA_EXPORTACION}")
        except Exception as e:
            print(f"\n❌ ERROR al exportar a Excel. Asegúrate de que el archivo no esté abierto y la ruta sea válida. {e}")
    else:
        print("\n🛑 El DataFrame final está vacío. Verifica la estructura de la API o las rutas de archivos.")