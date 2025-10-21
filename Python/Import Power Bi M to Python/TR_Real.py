import pandas as pd
import numpy as np
import os
from functools import reduce

# ==============================================================================
# CONFIGURACIÓN Y RUTAS
# ==============================================================================
# Rutas de Archivos: Se usan las rutas locales proporcionadas por el usuario.
RUTA_EXT_DATOS = r"C:\Users\USUARIO\Downloads\Ext_Datos.csv"
RUTA_CATALOGO = r"C:\Users\USUARIO\Downloads\ConceptosReporte.xlsx"
RUTA_SALIDA_EXCEL = r"C:\Users\USUARIO\Downloads\Reporte_Consolidado.xlsx" # Archivo de salida

# Claves: Se utiliza el nombre estandarizado (con underscore) en la lógica de Pandas
JOIN_COLS_ORIGEN = ["Concepto_Reporte", "Reporte"]
JOIN_COLS_CATALOGO = ["CONCEPTO REPORTE", "SECCION"]

GRUPO_COLS = ["Fecha", "planta", "Division", "Reporte", "Concepto", "Unidad", "Orden", "Concepto2", "Concepto Capacidad"]
SALDO_COLS = ["Real", "Meta", "Valor Tope", "Valor Planta", "Valor Transito", "Valor Capacidad", "Valor Capacidad 91"]

# Mapeo de nombres originales del catálogo (Simula la expansión en M)
EXPANSION_RENAME_MAP = {
    "CONCEPTO": "Concepto",
    "UNIDAD": "Unidad",
    "ORDEN": "Orden",
    "CONCEPTO 2": "Concepto2",
    "TIPO SALDO": "TipoSaldo", 
    "CONCEPTO CAPACIDAD": "Concepto Capacidad"
}

# Mapeo para el pivot (Simula FCN_TipoSaldoAColumnas)
TIPO_SALDO_MAP_COLUMNAS = {
    "REAL": "Real",
    "META": "Meta",
    "VALOR TOPE": "Valor Tope",
    "VALOR PLANTA": "Valor Planta",
    "VALOR TRANSITO": "Valor Transito",
    "CAPACIDAD": "Valor Capacidad",
    "CAPACIDAD 91": "Valor Capacidad 91",
}


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

def cargar_datos():
    """Carga los datos y realiza la limpieza y el renombrado inicial."""
    print("Iniciando carga de datos...")
    
    # Cargar Origen (Ext_Datos)
    try:
        df_origen = pd.read_csv(RUTA_EXT_DATOS)
        
        # 💥 CORRECCIÓN CRÍTICA (Simula Table.RenameColumns y corrige el error de columna)
        columnas_renombrar = {"SEGMENTO": "Division"}
        if "Concepto Reporte" in df_origen.columns: # Detecta el nombre con espacio
            columnas_renombrar["Concepto Reporte"] = "Concepto_Reporte" # Renombra a underscore
            
        df_origen.rename(columns=columnas_renombrar, inplace=True)
        
        # Limpieza y conversión de 'Valor' a numérico
        if 'Valor' in df_origen.columns:
            df_origen['Valor'] = df_origen['Valor'].astype(str)
            df_origen['Valor'] = (
                df_origen['Valor']
                .str.replace('$', '', regex=False).str.replace(' ', '', regex=False)
                .str.replace(',', '', regex=False).str.replace('.', ',', regex=False)
                .str.replace(',', '.', regex=False)
            )
            df_origen['Valor'] = pd.to_numeric(df_origen['Valor'], errors='coerce').fillna(0)
            
        print(f"✅ Ext_Datos cargado y renombrado ({len(df_origen)} filas).")
    except Exception as e:
        print(f"❌ ERROR al cargar Ext_Datos: {e}")
        return None, None

    # Cargar Catálogo (ConceptosReporte)
    try:
        # Se asume lectura de Excel por la extensión .xlsx en la ruta
        df_catalogo = pd.read_excel(RUTA_CATALOGO)
        print(f"✅ Catálogo ConceptosReporte cargado ({len(df_catalogo)} filas).")
    except Exception as e:
        print(f"❌ ERROR al cargar Catálogo: {e}")
        return df_origen, None

    return df_origen, df_catalogo

# ==============================================================================
# TRANSFORMACIÓN (Equivalente a la lógica M)
# ==============================================================================

def transformar_logica_m(df_origen, df_catalogo):
    """Aplica la lógica de transformación M a los DataFrames de Pandas."""
    if df_origen is None or df_catalogo is None:
        return pd.DataFrame()

    # 1. Normalización de Claves (Esencial para el Join. M lo hace implícitamente mejor)
    print("\n--- NORMALIZACIÓN DE CLAVES ---")
    for col in JOIN_COLS_ORIGEN:
        df_origen[col] = df_origen[col].astype(str).str.upper().str.strip()
    for col in JOIN_COLS_CATALOGO:
        df_catalogo[col] = df_catalogo[col].astype(str).str.upper().str.strip()
    
    # 2. Join (Consultas combinadas)
    df_join = pd.merge(
        df_origen,
        df_catalogo,
        left_on=JOIN_COLS_ORIGEN,
        right_on=JOIN_COLS_CATALOGO,
        how='inner' # JoinKind.Inner
    )
    
    if df_join.empty:
        print("❌ ERROR DE JOIN: El DataFrame combinado está vacío. (No hay coincidencias después de normalizar).")
        return pd.DataFrame()

    print(f"✅ Join con Catálogo completado ({len(df_join)} filas).")
    
    # 3. Expansión y Renombrado (Se expandió ConceptosReporte)
    df_expandido = df_join.copy().rename(columns=EXPANSION_RENAME_MAP)
    
    # 4. Preparación para Pivot
    df_expandido['TipoSaldo'] = df_expandido['TipoSaldo'].astype(str).str.upper().str.strip()
    
    valid_tipos_encontrados = [t for t in df_expandido['TipoSaldo'].unique() if t in TIPO_SALDO_MAP_COLUMNAS.keys()]

    if not valid_tipos_encontrados:
         print("❌ ¡FALLO CRÍTICO!: La columna 'TipoSaldo' está vacía o no tiene valores válidos.")
         return pd.DataFrame()

    df_pivot_source = df_expandido[df_expandido['TipoSaldo'].isin(valid_tipos_encontrados)].copy()
    df_pivot_source['Columna Final'] = df_pivot_source['TipoSaldo'].map(TIPO_SALDO_MAP_COLUMNAS)

    # 5. Pivot y Agrupación (FCN_TipoSaldoAColumnas, ConcatenaSaldos y AgrupaConceptos)
    # pd.pivot_table reemplaza la creación de 7 tablas, la concatenación y la agrupación.
    cols_base = [col for col in GRUPO_COLS if col in df_expandido.columns]
    
    try:
        df_agrupado = pd.pivot_table(
            df_pivot_source,
            index=cols_base, 
            columns='Columna Final', 
            values='Valor', 
            aggfunc='sum' 
        ).fillna(0).reset_index() 
        
        print("✅ Pivot y Agrupación consolidadas completadas.")
    except Exception as e:
        print(f"❌ ERROR durante la operación pivot/groupby: {e}")
        return pd.DataFrame()
    
    # Asegurar que todas las columnas de saldo existen (simula el null a 0 de Table.ReplaceValue)
    for col in SALDO_COLS:
        if col not in df_agrupado.columns:
            df_agrupado[col] = 0.0

    df_agrupado[SALDO_COLS] = df_agrupado[SALDO_COLS].astype(float)
    
    # 6. Filtrado (QuitaValorCero)
    condicion_no_cero = (df_agrupado[SALDO_COLS].abs().sum(axis=1) != 0)
    df_final = df_agrupado[condicion_no_cero].copy()
    
    # 7. Reordenamiento (Columnas reordenadas)
    df_final = df_final.reindex(columns=[col for col in GRUPO_COLS + SALDO_COLS if col in df_final.columns])

    print(f"\n✅ Eliminadas filas con saldos totales en cero. Filas finales: {len(df_final)}")
    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    df_origen, df_catalogo = cargar_datos()
    
    if df_origen is not None and df_catalogo is not None:
        df_reporte_final = transformar_logica_m(df_origen, df_catalogo)
        
        if not df_reporte_final.empty:
            print("\n================ RESULTADO FINAL DEL REPORTE ================")
            
            # 🚀 EXPORTAR A EXCEL
            try:
                df_reporte_final.to_excel(RUTA_SALIDA_EXCEL, index=False)
                print(f"✅ Exportación a Excel exitosa: El reporte se guardó en:\n{RUTA_SALIDA_EXCEL}")
            except Exception as e:
                print(f"❌ ERROR al guardar el archivo Excel: {e}")
            
            print(f"\nFilas resultantes: {len(df_reporte_final)}")
            print("\nPrimeras 5 filas:")
            print(df_reporte_final.head().to_markdown(index=False))
        else:
            print("\n🛑 El DataFrame final está vacío. Revise la sección de DEPURACIÓN ⬆️")
    else:
        print("\n🛑 Falló la carga inicial de datos. No se pudo iniciar la transformación.")