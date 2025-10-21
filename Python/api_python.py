import pandas as pd
import requests
import json
import numpy as np 
from typing import List, Dict, Any, Optional
from unidecode import unidecode # Necesario para limpiar claves de merge

# --- 1. CONFIGURACIÓN ---
API_URL = "https://kpis.grupo-ortiz.site/Controllers/apiController.php?op=api"
HEADERS = {'Accept': 'application/json'}
ID_VARS = ["date", "planta", "SEGMENTO"] 
REPORTE_COLS = ["VENTAS 360", "PRODUCCION 360", "INVENTARIOS 360", "DESEMPEÑO 360"]

# Columnas de saldo que serán desdinamizadas (Unpivot) para crear 'TIPO SALDO'
UNPIVOT_VALUE_COLS = ["REAL", "META", "CAPACIDAD", "CAPACIDAD 91", "VALOR TOPE", "VALOR PLANTA", "VALOR TRANSITO"] 

# ==================================================================================
# --- CONTENIDO DEL CSV CARGADO (SIMULACIÓN DE LECTURA) ---
# ==================================================================================
# Utiliza el contenido literal que proporcionaste
CATALOGO_CSV_CONTENT = """
,,,,,,,,,,,,,,,
,CONCEPTO,REAL,META,CAPACIDAD,VALOR TOPE,VALOR PLANTA,VALOR TRANSITO,DIAS LABORABLES,DIAS LABORABLES CAPACIDADES,CONCEPTO CAPACIDAD,UNIDAD,SECCION,ORDEN,CONCEPTO 2,FLAG
,💵 Ventas,IMPORTE TOTAL,META VENTA IMPORTE,,,,,DIAS VENTA IMPORTE,,,$,Desempeño 360,1,💵 Ventas,
,📉 Costo de Ventas,COSTO TOTAL,META COSTO,,,,,DIAS COSTO,,,$,Desempeño 360,2,📉 Costo de Ventas,
,💰 Gasto de Operación,GASTO OPERATIVO,META GASTO,,,,,DIAS GASTO,,,$,Desempeño 360,3,💰 Gasto de Operación,
,💸 Utilidad Neta,FLUJO NETO,META UTILIDAD,,,,,DIAS UTILIDAD,,,$,Desempeño 360,4,💸 Utilidad Neta,
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,CONCEPTO,REAL,META,CAPACIDAD,VALOR TOPE,VALOR PLANTA,VALOR TRANSITO,DIAS LABORABLES,DIAS LABORABLES CAPACIDADES,CONCEPTO CAPACIDAD,UNIDAD,SECCION,ORDEN,CONCEPTO 2,FLAG
,1A kg,PRIMERA PRODUCCION KG,META PRIMERA KG,,,,,DIAS PRIMERA KG,,,kg,Produccion 360,5,1A,1
,1A pz,PRIMERA PRODUCCION PZ,META PRIMERA PZS,,,,,DIAS PRIMERA PZS,,,pz,Produccion 360,6,1A,1
,2DA kg,SEGUNDA PRODUCCION KG,META SEGUNDA KG,,,,,DIAS SEGUNDA KG,,,kg,Produccion 360,7,2DA,1
,2DA pz,SEGUNDA PRODUCCION PZ,META SEGUNDA PZS,,,,,DIAS SEGUNDA PZS,,,pz,Produccion 360,8,2DA,1
,Desperdicio kg,DESPERDICIO PRODUCCION KG,META DESPERDICIO KG,,,,,DIAS DESPERDICIO KG,,,kg,Produccion 360,9,DESPERDICIO,1
,PELETIZADO KILOS,PELETIZADO P,META PELETIZADO P,,,,,DIAS PELETIZADO P,,,kg,Produccion 360,10,PELETIZADO KILOS,1
,PRODUCCION TOTAL,PRODUCCION TOTAL KG,META PRODUCCION TOTAL KG,,,,,,,,kg,Produccion 360,11,PRODUCCION TOTAL,1
,KG POR PERSONA,KG POR PERSONA,META KG POR PERSONA,,,,,DIAS KG POR PERSONA,,,KILOS,Produccion 360,12,KG POR PERSONA,1
,PZ POR PERSONA,PZ POR PERSONA,META PZ POR PERSONA,,,,,DIAS PZ POR PERSONA,,,PIEZAS,Produccion 360,13,PZ POR PERSONA,1
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,CONCEPTO,REAL,META,CAPACIDAD,VALOR TOPE,VALOR PLANTA,VALOR TRANSITO,DIAS LABORABLES,DIAS LABORABLES CAPACIDADES,CONCEPTO CAPACIDAD,UNIDAD,SECCION,ORDEN,CONCEPTO 2,FLAG
,EXTRUDER,EXTRUDER,META EXTRUDER,META CAPACIDAD EXTRUDER,,,,DIAS EXTRUDER,DIAS CAPACIDAD EXTRUDER,META CAPACIDAD EXTRUDER,KILOS,Produccion 360,1000,EXTRUDER,2
,EXTRUDER MONOFILAMENTO,EXTRUDER MONOFILAMENTO,META EXTRUDER MONOFILAMENTO,META CAPACIDAD EXTRUDER MONOFILAMENTO,,,,DIAS EXTRUDER MONOFILAMENTO,DIAS CAPACIDAD EXTRUDER MONOFILAMENTO,META CAPACIDAD EXTRUDER MONOFILAMENTO,KILOS,Produccion 360,1000,EXTRUDER MONOFILAMENTO,2
,EXTRUDER RAFIA FIBRILADA,EXTRUDER RAFIA FIBRILADA,META EXTRUDER RAFIA FIBRILADA,META CAPACIDAD EXTRUDER RAFIA FIBRILADA,,,,DIAS EXTRUDER RAFIA FIBRILADA,DIAS CAPACIDAD EXTRUDER RAFIA FIBRILADA,META CAPACIDAD EXTRUDER RAFIA FIBRILADA,KILOS,Produccion 360,1000,EXTRUDER RAFIA FIBRILADA,2
,EXTRUDER RAFIA SOPLADA,EXTRUDER RAFIA SOPLADA,META EXTRUDER RAFIA SOPLADA,META CAPACIDAD EXTRUDER RAFIA SOPLADA,,,,DIAS EXTRUDER RAFIA SOPLADA,DIAS CAPACIDAD EXTRUDER RAFIA SOPLADA,META CAPACIDAD EXTRUDER RAFIA SOPLADA,KILOS,Produccion 360,1000,EXTRUDER RAFIA SOPLADA,2
,TELARES,TELARES MT,META TELARES,META CAPACIDAD TELARES,,,,DIAS TELARES,DIAS CAPACIDAD TELARES,META CAPACIDAD TELARES,METROS,Produccion 360,1000,TELARES,2
,TELAR JUMBO,TELAR JUMBO MT,META TELARES JUMBO,META CAPACIDAD TELAR JUMBO,,,,DIAS TELARES JUMBO,DIAS CAPACIDAD TELAR JUMBO,META CAPACIDAD TELAR JUMBO,METROS,Produccion 360,1000,TELAR JUMBO,2
,VALVULADO,VALVULADO,META VALVULADO,META CAPACIDAD VALVULADO,,,,DIAS VALVULADO,DIAS CAPACIDAD VALVULADO,META CAPACIDAD VALVULADO,PIEZAS,Produccion 360,1000,VALVULADO,2
,LAMINADO,LAMINADO,META LAMINADO,META CAPACIDAD LAMINADO,,,,DIAS LAMINADO,DIAS CAPACIDAD LAMINADO,META CAPACIDAD LAMINADO,METROS,Produccion 360,1000,LAMINADO,2
,IMPRESION,IMPRESION,META IMPRESION,META CAPACIDAD IMPRESION,,,,DIAS IMPRESION,DIAS CAPACIDAD IMPRESION,META CAPACIDAD IMPRESION,METROS,Produccion 360,1000,IMPRESION,2
,CONFECCION,CONFECCION,META CONFECCION,META CAPACIDAD CONFECCION,,,,DIAS CONFECCION,DIAS CAPACIDAD CONFECCION,META CAPACIDAD CONFECCION,PIEZAS,Produccion 360,1000,CONFECCION,2
,CABLEADORAS,PRODUCCION CABLE KG,META CABLEADORAS,META CAPACIDAD CABLEADORAS,,,,DIAS CABLEADORAS,DIAS CAPACIDAD CABLEADORAS,META CAPACIDAD CABLEADORAS,KILOS,Produccion 360,1000,CABLEADORAS,2
,BOLERAS RAFIAS FIBRILADA,BOLERA RAFIA FIBRILADA,META BOLERAS RAFIAS FIBRILADA,META CAPACIDAD BOLERAS RAFIAS FIBRILADA,,,,DIAS BOLERAS RAFIAS FIBRILADA,DIAS CAPACIDAD BOLERAS RAFIAS FIBRILADA,META CAPACIDAD BOLERAS RAFIAS FIBRILADA,KILOS,Produccion 360,1000,BOLERAS RAFIAS FIBRILADA,2
,BOLERAS RAFIA SOPLADA,BOLERA RAFIA SOPLADA,META BOLERAS RAFIA SOPLADA,META CAPACIDAD BOLERAS RAFIA SOPLADA,,,,DIAS BOLERAS RAFIA SOPLADA,DIAS CAPACIDAD BOLERAS RAFIA SOPLADA,META CAPACIDAD BOLERAS RAFIA SOPLADA,KILOS,Produccion 360,1000,BOLERAS RAFIA SOPLADA,2
,OVER,OVER,META OVER,META CAPACIDAD OVER,,,,DIAS OVER,DIAS CAPACIDAD OVER,META CAPACIDAD OVER,PIEZAS,Produccion 360,1000,OVER,2
,AUTOMATEX,AUTOMATEX PZ,META AUTOMATEX,META CAPACIDAD AUTOMATEX,,,,DIAS AUTOMATEX,DIAS CAPACIDAD AUTOMATEX,META CAPACIDAD AUTOMATEX,PIEZAS,Produccion 360,1000,AUTOMATEX,2
,PRENSA ,PRENSA PZ,META PRENSA,META CAPACIDAD PRENSA,,,,DIAS PRENSA,DIAS CAPACIDAD PRENSA,META CAPACIDAD PRENSA,PIEZAS,Produccion 360,1000,PRENSA,2
,FFS,FFS,META FFS,META CAPACIDAD FFS,,,,DIAS FFS,DIAS CAPACIDAD FFS,META CAPACIDAD FFS,KILOS,Produccion 360,1000,FFS,2
,SML0,SML0 KG,META SML0,META CAPACIDAD SML0,,,,DIAS SML0,DIAS CAPACIDAD SML0,META CAPACIDAD SML0,KILOS,Produccion 360,1000,SML0,2
,SML1,SML1 KG,META SML1,META CAPACIDAD SML1,,,,DIAS SML1,DIAS CAPACIDAD SML1,META CAPACIDAD SML1,KILOS,Produccion 360,1000,SML1,2
,SML2,SML2 KG,META SML2,META CAPACIDAD SML2,,,,DIAS SML2,DIAS CAPACIDAD SML2,META CAPACIDAD SML2,KILOS,Produccion 360,1000,SML2,2
,SML4,SML4 KG,META SML4,META CAPACIDAD SML4,,,,DIAS SML4,DIAS CAPACIDAD SML4,META CAPACIDAD SML4,KILOS,Produccion 360,1000,SML4,2
,PREESTIRADO,PREESTIRADO KG,META PREESTIRADO,META CAPACIDAD PREESTIRADO,,,,DIAS PREESTIRADO,DIAS CAPACIDAD PREESTIRADO,META CAPACIDAD PREESTIRADO,KILOS,Produccion 360,1000,PREESTIRADO,2
,REBOBINADO,REBOBINADO KG,META REBOBINADO,META CAPACIDAD REBOBINADO,,,,DIAS REBOBINADO,DIAS CAPACIDAD REBOBINADO,META CAPACIDAD REBOBINADO,KILOS,Produccion 360,1000,REBOBINADO,2
,CORTADORA,CORTADORA KG,META CORTADORA,META CAPACIDAD CORTADORA,,,,DIAS CORTADORA,DIAS CAPACIDAD CORTADORA,META CAPACIDAD CORTADORA,KILOS,Produccion 360,1000,CORTADORA,2
,BOLSEO,BOLSEO,META BOLSEO,META CAPACIDAD BOLSEO,,,,DIAS BOLSEO,DIAS CAPACIDAD BOLSEO,META CAPACIDAD BOLSEO,PIEZAS,Produccion 360,1000,BOLSEO,2
,PELETIZADORA,PELETIZADORA,META PELETIZADORA,META CAPACIDAD PELETIZADORA,,,,DIAS PELETIZADORA,DIAS CAPACIDAD PELETIZADORA,META CAPACIDAD PELETIZADORA,KILOS,Produccion 360,1000,PELETIZADORA,2
,LAVADORA,LAVADORA,META LAVADORA,META CAPCIDAD LAVADORA,,,,DIAS LAVADORA,DIAS CAPCIDAD LAVADORA,META CAPCIDAD LAVADORA,KILOS,Produccion 360,1000,LAVADORA,2
,RECTA,RECTA,META RECTA,META CAPACIDAD RECTA,,,,DIAS RECTA,DIAS CAPACIDAD RECTA,META CAPACIDAD RECTA,METROS,Produccion 360,1000,RECTA,2
,CORTE,CORTE,META CORTE,META CAPACIDAD CORTE,,,,DIAS CORTE,DIAS CAPACIDAD CORTE,META CAPACIDAD CORTE,METROS,Produccion 360,1000,CORTE,2
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,CONCEPTO,REAL,META,CAPACIDAD,VALOR TOPE,VALOR PLANTA,VALOR TRANSITO,DIAS LABORABLES,DIAS LABORABLES CAPACIDADES,CONCEPTO CAPACIDAD,UNIDAD,SECCION,ORDEN,CONCEPTO 2,FLAG
,EFICIENCIA KG,KG POR PERSONA,-,,,,,,,,KILOS,Produccion 360,20,EFICIENCIA,
,EFICIENCIA PZ,PZ POR PERSONA,-,,,,,,,,PIEZAS,Produccion 360,21,EFICIENCIA,
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,CONCEPTO,REAL,META,CAPACIDAD,VALOR TOPE,VALOR PLANTA,VALOR TRANSITO,DIAS LABORABLES,DIAS LABORABLES CAPACIDADES,CONCEPTO CAPACIDAD,UNIDAD,SECCION,ORDEN,CONCEPTO 2,FLAG
,PRECIO DE VENTA,PRECIO DE VENTA POR KILO,-,,,,,-,,,$,Ventas 360,22,PRECIO DE VENTA,
,COSTO VENTA,COSTO POR KILO DE VENTA,-,,,,,-,,,$,Ventas 360,23,COSTO VENTA,
,COSTO PRODUCCION,COSTO PRODUCCION POR KILO,-,,,,,-,,,$,Ventas 360,24,COSTO PRODUCCION,
,PRECIO PRODUCCION,PRECIO DE VENTA POR KILO PRODUCCION,-,,,,,-,,,$,Ventas 360,25,PRECIO PRODUCCION,
,PEDIDOS COLOCADOS KG,PEDIDOS COLOCADOS KG,-,,,,,-,,,KILOS,Ventas 360,26,PEDIDOS COLOCADOS,
,PEDIDOS COLOCADOS PZ,PEDIDOS COLOCADOS PZ,-,,,,,-,,,PIEZAS,Ventas 360,27,PEDIDOS COLOCADOS,
,FACTURADO KG,FACTURADO KG,META VENTA KILOS FACTURADO,,,,,DIAS FACTURADO KG,,,KILOS,Ventas 360,28,FACTURADO,
,FACTURADO PZ,FACTURADO PZ,META VENTA PIEZAS FACTURADO,,,,,DIAS FACTURADO PZ,,,PIEZAS,Ventas 360,29,FACTURADO,
,FABRICADO KG,FABRICADO KG,-,,,,,-,,,KILOS,Ventas 360,30,FABRICADO,
,FABRICADO PZ,FABRICADO PZ,-,,,,,-,,,PIEZAS,Ventas 360,31,FABRICADO,
,PRECIO VENTA PELETIZADO,PRECIO VENTA PELETIZADO,-,,,,,-,,,$,Ventas 360,32,PRECIO VENTA PELETIZADO,
,PRECIO VENTA TRITURADO,PRECIO VENTA TRITURADO,-,,,,,-,,,$,Ventas 360,33,PRECIO VENTA TRITURADO,
,PRECIO VENTA LAVADO,PRECIO VENTA LAVADO,-,,,,,-,,,$,Ventas 360,34,PRECIO VENTA LAVADO,
,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,
,CONCEPTO,REAL,META,CAPACIDAD,VALOR TOPE,VALOR PLANTA,VALOR TRANSITO,DIAS LABORABLES,DIAS LABORABLES CAPACIDADES,CONCEPTO CAPACIDAD,UNIDAD,SECCION,ORDEN,CONCEPTO 2,FLAG
,PT KG,,,,PT INVENTARIO TOPE KG,PT INVENTARIO KG,-,DIAS STOCK PT KG,,,KILOS,Inventarios 360,101,PT KG,
,PT PZ,,,,PT INVENTARIO TOPE PZ,PT INVENTARIO PZ,-,DIAS STOCK PT PZS,,,PIEZAS,Inventarios 360,102,PT PZ,
,2DA KG,,,,-,SEGUNDA INVENTARIO KG,-,,,,KILOS,Inventarios 360,103,2DA KG,
,2DA PZ,,,,-,SEGUNDA INVENTARIO PZ,-,,,,PIEZAS,Inventarios 360,104,2DA PZ,
,MP KG,,,,MP INVENTARIO TOPE,MP INVENTARIO PLANTA,MP INVENTARIO TRANSITO,DIAS MAXIMO MP,,,KILOS,Inventarios 360,105,MP KG,
,DESPERDICIO KG,,,,META DESPERDICIO INVENTARIO,DESPERDICIO INVENTARIO KG,-,DIAS DESPERDICIO INVENTARIO KG,,,KILOS,Inventarios 360,106,DESPERDICIO KG,
,PELETIZADO KG,,,,META PELETIZADO KG,PELETIZADO KG,-,DIAS STOCK INVENTARIO PELETIZADO,,,KILOS,Inventarios 360,107,PELETIZADO KG,
"""


# ==================================================================================
# --- FUNCIONES DE CARGA Y LIMPIEZA DEL CATÁLOGO (ConceptosReporte) ---
# ==================================================================================

def cargar_catalogo_desde_csv_cargado(csv_content: str) -> pd.DataFrame:
    """
    Procesa el contenido del CSV cargado, maneja múltiples encabezados y el unpivot 
    para crear la tabla ConceptosReporte (el catálogo).
    """
    print("   📥 Iniciando Carga Robusta y Limpieza del Catálogo CSV...")
    try:
        # 1. Leer el contenido como DataFrame, sin encabezados y sin saltar filas
        from io import StringIO
        df_raw = pd.read_csv(StringIO(csv_content), header=None, skiprows=0, sep=',', engine='python', skipinitialspace=True)
        
        # 2. Encontrar todas las filas de encabezado (donde la columna B es 'CONCEPTO')
        # La columna 0 es la primera coma vacía, la columna 1 es 'CONCEPTO'
        header_indices = df_raw[df_raw.iloc[:, 1].astype(str).str.strip().str.upper() == 'CONCEPTO'].index.tolist()
        
        if not header_indices:
            raise ValueError("No se encontraron filas de encabezado 'CONCEPTO' en la columna B del CSV.")

        df_list = []
        
        # 3. Procesar cada sección del catálogo
        for i, start_index in enumerate(header_indices):
            
            # Determinar el final de esta sección (inicio de la siguiente o final del archivo)
            end_index = header_indices[i+1] if i + 1 < len(header_indices) else len(df_raw)
            
            # Obtener el sub-DataFrame
            df_section = df_raw.iloc[start_index:end_index].copy()
            
            # Promover encabezados
            new_header = df_section.iloc[0].astype(str).str.strip().str.upper().tolist()
            df_section = df_section[1:].copy()
            df_section.columns = new_header
            df_section = df_section.reset_index(drop=True)

            # Quitar filas completamente vacías o con CONCEPTO vacío
            df_section = df_section[df_section['CONCEPTO'].astype(str).str.strip() != ''].copy()
            
            df_list.append(df_section)

        # 4. Concatenar todas las secciones limpias
        df_base = pd.concat(df_list, ignore_index=True)
        
        # 5. Estandarizar columnas a utilizar
        cols_to_keep_upper = [col.upper() for col in ["CONCEPTO", "REAL", "META", "CAPACIDAD", "VALOR TOPE", 
                                                      "VALOR PLANTA", "VALOR TRANSITO", 
                                                      "UNIDAD", "SECCION", "ORDEN", "CONCEPTO 2", "CONCEPTO CAPACIDAD"]]
        
        df_base = df_base.filter(items=cols_to_keep_upper, axis=1).copy()
        df_base = df_base.replace({np.nan: ''})
        
        # Forzar Orden a numérico, manejando errores (como '1000' o '-'/vacío)
        if 'ORDEN' in df_base.columns:
            df_base['ORDEN'] = pd.to_numeric(df_base['ORDEN'], errors='coerce').astype('Int64', errors='ignore')
            
        # 6. Desdinamización (Unpivot) para simular la estructura de ConceptosReporte con TipoSaldo
        id_cols_unpivot = [col for col in df_base.columns if col not in [c.upper() for c in UNPIVOT_VALUE_COLS]]
        
        df_unpivot = df_base.melt(
            id_vars=id_cols_unpivot,
            value_vars=[col.upper() for col in UNPIVOT_VALUE_COLS if col.upper() in df_base.columns],
            var_name="TIPO SALDO",
            value_name="CONCEPTO REPORTE"
        )
        
        # 7. Limpieza final de filas generadas por el Unpivot (donde el concepto real del reporte es '-')
        df_unpivot = df_unpivot[
            (df_unpivot['CONCEPTO REPORTE'].astype(str).str.strip() != '') & 
            (df_unpivot['CONCEPTO REPORTE'].astype(str).str.strip() != '-') &
            (~df_unpivot['CONCEPTO REPORTE'].astype(str).str.upper().isin(['NAN', 'NONE']))
        ].copy()
        
        df_unpivot['TIPO SALDO'] = df_unpivot['TIPO SALDO'].astype(str).str.upper()
        
        print(f"   ✔️ Catálogo (ConceptosReporte) cargado y transformado con {len(df_unpivot)} filas.")
        return df_unpivot

    except Exception as e:
        print(f"❌ Error al procesar Catálogo CSV: {e}")
        return pd.DataFrame()


# ==================================================================================
# --- FUNCIONES DE TRANSFORMACIÓN Y EXTRACCIÓN (MANTENIDAS) ---
# ==================================================================================
# Las funciones de extracción y transformación se mantienen, pero la lógica de merge
# se ha copiado del script anterior para asegurar que se ejecuta correctamente
# con los nuevos DataFrames:

# --- Funciones de Extracción (Manteniendo las correcciones anteriores) ---
def extraer_datos_api(url: str, headers: Dict[str, str]) -> pd.DataFrame:
    # ... (Se mantiene la función extraer_datos_api) ...
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
    # ... (Se mantiene la función expandir_y_unificar_reporte) ...
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
    
    df_unpivot['Reporte'] = nombre_reporte.replace(" ", "").upper()
    
    return df_unpivot.dropna(subset=id_cols_present, how='all')

# --- Función de Transformación del Script M (Se mantiene la lógica anterior) ---
def crear_tabla_procesada_catalogos(df_api_limpio: pd.DataFrame, df_catalogo: pd.DataFrame) -> pd.DataFrame:
    """Implementa en Python la lógica del script de Power Query (Lenguaje M)."""
    if df_api_limpio.empty or df_catalogo.empty:
        return pd.DataFrame()

    print("\n   Creando Tabla Final: Procesando Catálogos y Consolidando Saldos (Lógica M)...")
    
    df_api_limpio['Valor'] = pd.to_numeric(df_api_limpio['Valor'], errors='coerce').fillna(0)
    df_temp = df_api_limpio.rename(columns={'SEGMENTO': 'Division'}).copy()
    
    # --- Estandarización de Claves para el Merge (CRÍTICO) ---
    def clean_key_normalized(s):
        if pd.isna(s): return ''
        s_str = str(s).strip()
        s_normalized = unidecode(s_str).upper()
        # Limpieza de caracteres que pueden variar entre la API y el Catálogo
        s_cleaned = s_normalized.replace(" ", "").replace("360", "").replace("$", "").replace("💵", "").replace("📉", "").replace("💰", "").replace("💸", "").replace(":", "").replace("-", "")
        return s_cleaned

    df_catalogo['SECCION_KEY'] = df_catalogo['SECCION'].apply(clean_key_normalized)
    df_temp['Reporte_KEY'] = df_temp['Reporte'].apply(clean_key_normalized)
    df_catalogo['CONCEPTO_REPORTE_KEY'] = df_catalogo['CONCEPTO REPORTE'].apply(clean_key_normalized)
    df_temp['Concepto_Reporte_KEY'] = df_temp['Concepto_Reporte'].apply(clean_key_normalized)
    
    # #"Consultas combinadas" (Table.NestedJoin - Inner)
    df_merged = pd.merge(
        df_temp,
        df_catalogo,
        left_on=['Concepto_Reporte_KEY', 'Reporte_KEY'],
        right_on=['CONCEPTO_REPORTE_KEY', 'SECCION_KEY'],
        how='inner' 
    )
    
    if df_merged.empty:
        print("❌ Fallo: La combinación (Merge) de datos de la API y el Catálogo resultó en 0 filas.")
        return pd.DataFrame()

    # #"Se expandió ConceptosReporte"
    df_expanded = df_merged.rename(columns={
        'CONCEPTO': 'Concepto', 'UNIDAD': 'Unidad', 'ORDEN': 'Orden',
        'CONCEPTO 2': 'Concepto2', 'TIPO SALDO': 'TipoSaldo',
        'CONCEPTO CAPACIDAD': 'Concepto Capacidad'
    }).drop(columns=['CONCEPTO REPORTE', 'SECCION', 'Concepto_Reporte_KEY', 'Reporte_KEY', 'CONCEPTO_REPORTE_KEY', 'SECCION_KEY'], errors='ignore')
    
    df_expanded.rename(columns={'date': 'Fecha', 'FECHA': 'Fecha'}, inplace=True, errors='ignore')
    
    ID_GROUP_KEYS = ["Fecha", "planta", "Division", "Reporte", "Concepto", "Unidad", "Orden", "Concepto2", "Concepto Capacidad"]
    ID_GROUP_KEYS_PRESENT = [col for col in ID_GROUP_KEYS if col in df_expanded.columns]

    saldos_map = {
        "REAL": "Real", "META": "Meta", "VALOR TOPE": "Valor Tope", 
        "VALOR PLANTA": "Valor Planta", "VALOR TRANSITO": "Valor Transito", 
        "CAPACIDAD": "Valor Capacidad", "CAPACIDAD 91": "Valor Capacidad 91"
    }
    tipos_saldos_existentes = df_expanded['TipoSaldo'].unique()
    saldos_map_final = {k: v for k, v in saldos_map.items() if k in tipos_saldos_existentes}

    # Simulación de FCN_TipoSaldoAColumnas y ConcatenaSaldos
    df_pivot_prep = []
    
    for tipo_saldo_m, nombre_columna_py in saldos_map_final.items():
        df_filtro = df_expanded[df_expanded['TipoSaldo'] == tipo_saldo_m].copy()
        
        if not df_filtro.empty:
            cols_to_drop = [c for c in ['Concepto_Reporte', 'TipoSaldo'] if c in df_filtro.columns]
            df_filtro = df_filtro.drop(columns=cols_to_drop, errors='ignore')
            
            cols_to_select = ID_GROUP_KEYS_PRESENT + ['Valor']
            df_filtro = df_filtro.filter(items=cols_to_select, axis=1).rename(columns={'Valor': nombre_columna_py})
            df_pivot_prep.append(df_filtro)
            
    if not df_pivot_prep: return pd.DataFrame()
    
    df_concatenado = df_pivot_prep[0]
    for df_next in df_pivot_prep[1:]:
        df_concatenado = pd.merge(df_concatenado, df_next, on=ID_GROUP_KEYS_PRESENT, how='outer')

    # #"Valor reemplazado" (Replace null, 0)
    sum_cols = list(saldos_map_final.values())
    df_concatenado[sum_cols] = df_concatenado[sum_cols].fillna(0) 
    
    # AgrupaConceptos (Table.Group)
    df_grouped = df_concatenado.groupby(ID_GROUP_KEYS_PRESENT, dropna=False).sum(numeric_only=True).reset_index()

    # QuitaValorCero (Table.SelectRows)
    filtro_no_cero = (df_grouped[sum_cols].sum(axis=1) != 0)
    df_final = df_grouped[filtro_no_cero].copy()
    
    final_cols_order = ["Fecha", "planta", "Division", "Reporte", "Concepto", "Unidad", "Orden", "Concepto2", "Concepto Capacidad"] + sum_cols
    cols_present = [col for col in final_cols_order if col in df_final.columns]
    df_final = df_final.filter(items=cols_present, axis=1)

    print(f"   ✔️ Tabla Final (Catálogo Procesado) creada con {len(df_final)} filas.")
    
    return df_final


# ==================================================================================
# --- EJECUCIÓN PRINCIPAL DEL FLUJO ETL ---
# ==================================================================================

if __name__ == '__main__':
    
    # 1. CARGA DEL CATÁLOGO DESDE EL CONTENIDO CSV
    df_catalogo_conceptos = cargar_catalogo_desde_csv_cargado(CATALOGO_CSV_CONTENT)
    
    if df_catalogo_conceptos.empty:
        print("🛑 Error Fatal: El catálogo de conceptos no pudo ser cargado o está vacío. Finalizando.")
        exit()
    
    # 2. PROCESAMIENTO API (Ext_Datos)
    print("\nIniciando Extracción y Aplanamiento de la API (Ext_Datos)...")
    df_base = extraer_datos_api(API_URL, HEADERS)
    
    if df_base.empty:
        print("🛑 No se pudo extraer la base de datos de la API. Finalizando.")
        exit()
    
    # Unificación de reportes de la API
    lista_tablas = []
    for col_expandir in REPORTE_COLS:
        df_reporte = expandir_y_unificar_reporte(df_base, col_expandir, col_expandir, [])
        if not df_reporte.empty:
            lista_tablas.append(df_reporte)

    if not lista_tablas:
        print("🛑 Ningún reporte pudo ser generado a partir de la API. Finalizando.")
        exit()

    df_api_limpio = pd.concat(lista_tablas, ignore_index=True)
    
    df_api_limpio.rename(columns={'date': 'Fecha'}, inplace=True)
    plantas_a_excluir = ["BRUCKNER", "DESCONOCIDO", "RECICLADORA"]
    df_api_limpio = df_api_limpio[~df_api_limpio['planta'].isin(plantas_a_excluir)].copy()
    df_api_limpio['Valor'] = pd.to_numeric(df_api_limpio['Valor'], errors='coerce')
    df_api_limpio['Fecha'] = pd.to_datetime(df_api_limpio['Fecha'], errors='coerce')
    print(f"✔️ Ext_Datos (API Detallada) generado con {len(df_api_limpio)} filas.")
    
    # 3. CREACIÓN DE LA TABLA FINAL (MERGE API + CATÁLOGO)
    df_procesado_catalogo = crear_tabla_procesada_catalogos(df_api_limpio, df_catalogo_conceptos)

    # 4. EXPORTACIÓN DEL RESULTADO
    output_filename = 'datos_api_procesados_final.csv'
    
    if not df_procesado_catalogo.empty:
        try:
            df_procesado_catalogo.to_csv(output_filename, index=False, encoding='utf-8')
            print("\n" + "="*80)
            print(f"✅ EXPORTACIÓN EXITOSA: Los datos finales se han guardado en: {output_filename}")
            print(f"Filas exportadas: {len(df_procesado_catalogo)}")
            print("="*80)
        except Exception as e:
            print(f"\n❌ ERROR al exportar el archivo CSV: {e}")
    else:
        print("\n🛑 No se pudo generar la tabla final de catálogos procesados. No se exportó ningún archivo.")