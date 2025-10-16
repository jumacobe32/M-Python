import pandas as pd
import requests
import json
from typing import List, Dict, Any, Optional

# ==================================================================================
# --- 1. CONFIGURACIÓN Y DEPENDENCIAS ---
# ==================================================================================

# 1.1 Configuración de la API (Tabla 1)
API_URL = "https://kpis.grupo-ortiz.site/Controllers/apiController.php?op=api"
HEADERS = {'Accept': 'application/json'}
ID_VARS = ["date", "planta", "SEGMENTO"] # Columnas de identificación
REPORTE_COLS = ["VENTAS 360", "PRODUCCION 360", "INVENTARIOS 360", "DESEMPEÑO 360"]

# 1.2 Configuración del Google Sheet (Fuente del Catálogo)
SHEET_ID = "1EK96qUKEW2dfnRBT7NfeVouAFouUXDOvHRVVGJ8gs34" 
EXPORT_URL_CATALOGO = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid=0"

# Columnas de saldo que serán desdinamizadas (Unpivot) para crear 'TIPO SALDO'
UNPIVOT_VALUE_COLS = ["REAL", "META", "CAPACIDAD", "CAPACIDAD 91"]

# 1.3 Simulación de ConceptosInventarios (Tabla anexa al Catálogo)
CONCEPTOS_INVENTARIOS_SIM = pd.DataFrame({
    'CONCEPTO': ['Inventario Inicial', 'Inventario Ajuste'],
    'UNIDAD': ['Ton', 'Ton'],
    'SECCION': ['Inventarios 360', 'Inventarios 360'],
    'ORDEN': [19, 29],
    'CONCEPTO 2': ['Inicio', 'Ajuste'],
    'CONCEPTO CAPACIDAD': ['No Aplica', 'No Aplica'],
    'TIPO SALDO': ['REAL', 'META'],
    'CONCEPTO REPORTE': ['Ton_Inicial', 'Ton_Ajuste']
})

# ==================================================================================
# --- 2. FUNCIONES DE EXTRACCIÓN Y PRE-PROCESAMIENTO ---
# ==================================================================================

def extraer_datos_api(url: str, headers: Dict[str, str]) -> pd.DataFrame:
    """Extrae datos reales de la API y los devuelve en formato DataFrame aplanado."""
    try:
        respuesta = requests.get(url, headers=headers)
        respuesta.raise_for_status()
        datos_json = respuesta.json()
        if not isinstance(datos_json, dict): return pd.DataFrame() 
        
        df_temp = pd.DataFrame(list(datos_json.items()), columns=['Name', 'Value'])
        list_rows = df_temp[df_temp['Value'].apply(lambda x: isinstance(x, list))]
        if list_rows.empty or not list_rows.iloc[0]['Value']: return pd.DataFrame() 
        data_list = list_rows.iloc[0]['Value']
        
        df_base = pd.json_normalize(data_list, sep='.')
        col_map = {}
        for col in df_base.columns:
            if col.startswith('GENERAL.'):
                col_map[col] = col.split('.')[-1]
        
        if col_map: df_base.rename(columns=col_map, inplace=True)
            
        report_cols_present = [col for col in REPORTE_COLS if any(c.startswith(f"{col}.") for c in df_base.columns)]
        if not report_cols_present: return pd.DataFrame()

        return df_base

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al extraer de la API: {e}")
        return pd.DataFrame() 
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado durante la extracción: {e}")
        return pd.DataFrame()

def expandir_y_unificar_reporte(
    df_base: pd.DataFrame, 
    columna_expandir: str, 
    nombre_reporte: str, 
    columnas_a_quitar: List[str] 
) -> pd.DataFrame:
    """Procesa las columnas anidadas quitando el prefijo y realizando el unpivot."""
    prefix = f"{columna_expandir}."
    report_data_cols = [col for col in df_base.columns if col.startswith(prefix)]

    if not report_data_cols: return pd.DataFrame()

    id_cols_present = [col for col in ID_VARS if col in df_base.columns]
    df_expand = df_base[id_cols_present + report_data_cols].copy()
    
    new_report_data_cols = [col.replace(prefix, '') for col in report_data_cols]
    col_map = dict(zip(report_data_cols, new_report_data_cols))
    df_expand.rename(columns=col_map, inplace=True)
    
    df_unpivot = df_expand.melt(
        id_vars=id_cols_present, 
        value_vars=new_report_data_cols, 
        var_name="Concepto_Reporte", 
        value_name="Valor" 
    ).dropna(subset=['Valor'])
    
    # La columna Reporte se crea aquí (Ej: VENTAS360)
    df_unpivot['Reporte'] = nombre_reporte.replace(" ", "").upper()
    
    return df_unpivot.dropna(subset=id_cols_present, how='all')

# ==================================================================================
# --- 3. FUNCIÓN DE CARGA DE CATÁLOGO (LÓGICA M) ---
# ==================================================================================

def cargar_catalogo_conceptos_m_logic(url: str, df_inventarios_sim: pd.DataFrame) -> pd.DataFrame:
    """
    Implementa la lógica del Lenguaje M para cargar y transformar la tabla ConceptosReporte 
    desde Google Sheets.
    """
    print(f"   📥 Iniciando carga y transformación de Catálogo desde: {url}")
    try:
        # 1. Origen: Lee la URL como CSV sin encabezado inicial (simula GoogleSheets.Contents)
        df_raw = pd.read_csv(url, encoding='utf-8', header=None, skiprows=0, sep=None, engine='python')

        # 2. Identifica la fila que contiene el encabezado 'CONCEPTO'
        header_row_index = -1
        for i, row in df_raw.iterrows():
            if 'CONCEPTO' in row.astype(str).str.upper().tolist():
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("No se encontró la fila de encabezados que contiene 'CONCEPTO'.")

        # 3. #"Encabezados promovidos": Usa la fila encontrada como encabezado
        df = df_raw.copy()
        df.columns = df.iloc[header_row_index] 
        df = df[header_row_index + 1:].reset_index(drop=True).copy() 
        
        # 4. #"Filas filtradas2": Quita la fila duplicada de encabezados
        df = df[df['CONCEPTO'].astype(str).str.upper() != 'CONCEPTO'].copy()
        
        # 5. #"Otras columnas quitadas1": Selecciona las columnas clave
        cols_to_keep = ["CONCEPTO", "REAL", "META", "UNIDAD", "SECCION", "ORDEN", 
                        "CONCEPTO 2", "CAPACIDAD", "CAPACIDAD 91", "CONCEPTO CAPACIDAD"]
        df_base = df.filter(items=cols_to_keep, axis=1).dropna(subset=['CONCEPTO']).copy()
        
        # 6. #"Tipo cambiado": Conversión de tipos
        df_base['ORDEN'] = pd.to_numeric(df_base['ORDEN'], errors='coerce').astype('Int64', errors='ignore') 
        
        # 7. #"Columna de anulación de dinamización" (Table.UnpivotOtherColumns)
        id_cols_unpivot = [col for col in cols_to_keep if col not in UNPIVOT_VALUE_COLS]

        df_unpivot = df_base.melt(
            id_vars=id_cols_unpivot,
            value_vars=UNPIVOT_VALUE_COLS,
            var_name="TIPO SALDO",
            value_name="CONCEPTO REPORTE"
        ).dropna(subset=['CONCEPTO REPORTE']) # Solo mantener donde hay valor
        
        # 8. #"Consulta anexada" (Table.Combine)
        df_final = pd.concat([df_unpivot, df_inventarios_sim], ignore_index=True)
        
        # 9. #"Filas filtradas" (Limpieza final)
        df_final = df_final[
            (df_final['CONCEPTO REPORTE'].astype(str).str.strip() != '') & 
            (df_final['CONCEPTO REPORTE'].astype(str).str.strip() != '-')
        ].copy()
        
        # Estandarizar 'TIPO SALDO'
        df_final['TIPO SALDO'] = df_final['TIPO SALDO'].str.upper()
        
        print(f"   ✔️ Catálogo (ConceptosReporte) cargado y transformado con {len(df_final)} filas.")
        return df_final

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al descargar Catálogo de Google Sheets: {e}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error al procesar Catálogo: {e}")
        return pd.DataFrame()


# ==================================================================================
# --- 4. FUNCIÓN DE TRANSFORMACIÓN (TABLA 3 - LÓGICA M) ---
# ==================================================================================

def crear_tabla_procesada_catalogos(df_api_limpio: pd.DataFrame, df_catalogo: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la lógica del Lenguaje M (join, unpivot inverso, group) para crear la Tabla 3.
    Implementa limpieza agresiva de conceptos y reportes para asegurar la coincidencia.
    """
    if df_api_limpio.empty or df_catalogo.empty:
        return pd.DataFrame()

    print("\n   Creando Tabla 3: Procesando Catálogos y Consolidando Saldos (Lógica M)...")
    
    # 0. Preparación y Origen (Ext_Datos)
    df_api_limpio['Valor'] = pd.to_numeric(df_api_limpio['Valor'], errors='coerce').fillna(0)
    
    # 1. #"Columnas con nombre cambiado1" (SEGMENTO -> Division)
    df_temp = df_api_limpio.rename(columns={'SEGMENTO': 'Division'}).copy()
    
    # --- ⚠️ CORRECCIÓN CLAVE: Estandarización de Claves para el Merge ---
    
    # Estandarización de los nombres de Reporte (SECCION) - Limpiamos 360 y espacios
    df_catalogo['SECCION'] = df_catalogo['SECCION'].astype(str).str.replace(" ", "").str.replace("360", "").str.upper()
    df_temp['Reporte'] = df_temp['Reporte'].astype(str).str.replace("360", "").str.upper()
    
    # Estandarización de los nombres de Concepto (CONCEPTO REPORTE) - Limpiamos espacios
    df_catalogo['CONCEPTO REPORTE'] = df_catalogo['CONCEPTO REPORTE'].astype(str).str.replace(" ", "").str.upper()
    df_temp['Concepto_Reporte'] = df_temp['Concepto_Reporte'].astype(str).str.replace(" ", "").str.upper()

    # --------------------------------------------------------------------

    # 2. #"Consultas combinadas" (Table.NestedJoin - Inner)
    df_merged = pd.merge(
        df_temp,
        df_catalogo,
        left_on=['Concepto_Reporte', 'Reporte'],
        right_on=['CONCEPTO REPORTE', 'SECCION'],
        how='inner' 
    )
    
    # Depuración: Si la tabla está vacía, el error es el contenido de los datos.
    if df_merged.empty:
        print("❌ Fallo: La combinación (Merge) de datos de la API y el Catálogo resultó en 0 filas.")
        print("  Verifique que los siguientes pares de claves coincidan exactamente (mayúsculas, sin espacios, sin 360):")
        
        # Muestra 5 ejemplos de las claves de cada lado
        api_claves = df_temp[['Concepto_Reporte', 'Reporte']].drop_duplicates().head(5).to_dict('records')
        cat_claves = df_catalogo[['CONCEPTO REPORTE', 'SECCION']].drop_duplicates().head(5).to_dict('records')
        
        print("\n  API Claves (Concepto_Reporte, Reporte):")
        for c in api_claves: print(f"    - ({c['Concepto_Reporte']}, {c['Reporte']})")
        
        print("\n  Catálogo Claves (CONCEPTO REPORTE, SECCION):")
        for c in cat_claves: print(f"    - ({c['CONCEPTO REPORTE']}, {c['SECCION']})")
        
        return pd.DataFrame()


    # 3. #"Se expandió ConceptosReporte"
    df_expanded = df_merged.rename(columns={
        'CONCEPTO': 'Concepto', 'UNIDAD': 'Unidad', 'ORDEN': 'Orden',
        'CONCEPTO 2': 'Concepto2', 'TIPO SALDO': 'TipoSaldo',
        'CONCEPTO CAPACIDAD': 'Concepto Capacidad'
    }).drop(columns=['CONCEPTO REPORTE', 'SECCION'], errors='ignore')
    
    ID_GROUP_KEYS = ["Fecha", "planta", "Division", "Concepto", "Unidad", "Orden", "Concepto2", "Concepto Capacidad"]
    
    # Definir los tipos de saldos y sus nombres de columna finales (simulando FCN_TipoSaldoAColumnas)
    saldos_map = {
        "REAL": "Real", "META": "Meta", "VALOR TOPE": "Valor Tope", 
        "VALOR PLANTA": "Valor Planta", "VALOR TRANSITO": "Valor Transito", 
        "CAPACIDAD": "Valor Capacidad", "CAPACIDAD 91": "Valor Capacidad 91"
    }
    sum_cols = list(saldos_map.values())

    # 4. Concatenación de saldos
    df_pivot_prep = []
    for tipo_saldo_m, nombre_columna_py in saldos_map.items():
        df_filtro = df_expanded[df_expanded['TipoSaldo'] == tipo_saldo_m].copy()
        
        if not df_filtro.empty:
            cols_to_drop = [c for c in ['Concepto_Reporte', 'TipoSaldo'] if c in df_filtro.columns]
            df_filtro = df_filtro.drop(columns=cols_to_drop, errors='ignore')
            
            cols_to_select = [col for col in ID_GROUP_KEYS if col in df_filtro.columns]
            df_filtro = df_filtro[cols_to_select + ['Valor']].rename(columns={'Valor': nombre_columna_py})
            df_pivot_prep.append(df_filtro)
        
    if not df_pivot_prep: return pd.DataFrame()
    df_concatenado = pd.concat(df_pivot_prep, ignore_index=True)

    # 5. Valor reemplazado y AgrupaConceptos (Table.Group)
    df_concatenado[sum_cols] = df_concatenado[sum_cols].fillna(0) # Reemplazo de valor (null->0)
    
    df_grouped = df_concatenado.groupby(ID_GROUP_KEYS, dropna=False).sum(numeric_only=True).reset_index()

    # 6. QuitaValorCero (Table.SelectRows)
    filtro_no_cero = (df_grouped[sum_cols].sum(axis=1) != 0)
    df_final = df_grouped[filtro_no_cero].copy()
    
    df_final.rename(columns={'date': 'Fecha'}, inplace=True) 

    print(f"   ✔️ Tabla 3 (Catálogo Procesado) creada con {len(df_final)} filas.")
    
    return df_final


# ==================================================================================
# --- 5. EJECUCIÓN PRINCIPAL DEL FLUJO ETL ---
# ==================================================================================

if __name__ == '__main__':
    
    # --- 1. PROCESAMIENTO API (Genera df_api_limpio - Tabla 1) ---
    print("Iniciando Extracción y Aplanamiento de la API (Tabla 1)...")
    df_base = extraer_datos_api(API_URL, HEADERS)
    
    if df_base.empty:
        print("🛑 No se pudo extraer la base de datos de la API. Finalizando.")
        exit()
    
    lista_tablas = []
    for col_expandir in REPORTE_COLS:
        df_reporte = expandir_y_unificar_reporte(df_base, col_expandir, col_expandir, [])
        if not df_reporte.empty:
            lista_tablas.append(df_reporte)

    if not lista_tablas:
        print("🛑 Ningún reporte pudo ser generado a partir de la API. Finalizando.")
        exit()
        
    df_final = pd.concat(lista_tablas, ignore_index=True)
    
    # Limpieza y Estandarización de la Tabla 1
    plantas_a_excluir = ["BRUCKNER", "DESCONOCIDO", "RECICLADORA"]
    df_api_limpio = df_final.copy()
    df_api_limpio.rename(columns={'date': 'Fecha'}, inplace=True)
    if 'planta' in df_api_limpio.columns:
        df_api_limpio = df_api_limpio[~df_api_limpio['planta'].isin(plantas_a_excluir)].copy()
    df_api_limpio['Valor'] = pd.to_numeric(df_api_limpio['Valor'], errors='coerce')
    df_api_limpio['Fecha'] = pd.to_datetime(df_api_limpio['Fecha'], errors='coerce')

    print(f"✔️ Tabla 1 (API Detallada) generada con {len(df_api_limpio)} filas.")
    
    # --- 2. CARGA DEL CATÁLOGO (ConceptosReporte) ---
    df_catalogo_conceptos = cargar_catalogo_conceptos_m_logic(EXPORT_URL_CATALOGO, CONCEPTOS_INVENTARIOS_SIM)
    
    if df_catalogo_conceptos.empty:
        print("🛑 No se pudo cargar el catálogo de conceptos. La Tabla 3 estará vacía.")
    
    # ----------------------------------------------------------------
    # --- 🎯 CREACIÓN DE LA TERCERA TABLA (TABLA 3) ---
    # ----------------------------------------------------------------
    
    df_procesado_catalogo = crear_tabla_procesada_catalogos(df_api_limpio, df_catalogo_conceptos)

    # --- 4. RESULTADOS FINALES ---
    print("\n" + "="*80)
    print("================ RESULTADOS FINALES ================")
    print("="*80)
    
    print(f"\n[TABLA 1: API - Datos Detallados ({len(df_api_limpio)} filas)]")
    print(df_api_limpio.head(5))
    
    print("\n" + "="*50 + "\n")
    