import pandas as pd
import os

# =========================================================================
# === CONFIGURACIÓN INICIAL PARA ARCHIVO EXCEL (.xlsx) ===
# =========================================================================
FILE_NAME = "Datos de power bi (1).xlsx" 
SHEET_NAME = "NUEVO DESEMPEÑO" 

# Saltamos la Fila 17 y leemos los datos a partir de la Fila 18.
SKIP_ROWS_COUNT = 17 
VALOR_FILTRO = 2 

# Nombres de las columnas de ORIGEN POR ÍNDICE (0-based, después de eliminar Columna 0):
IDX_FLAG = 8  # Columna 9 de M (para filtro)
IDX_CAPACIDAD = 9  # Columna 10 de M (para CONCEPTO)
IDX_DIAS = 11  # Columna 13 de M (para DIAS LABORABLES)

# Nombres de las columnas de DESTINO
NOMBRE_COLUMNA_CONCEPTO_FINAL = "CONCEPTO"
NOMBRE_COLUMNA_DIAS_FINAL = "DIAS LABORABLES"

#  MODIFICACIÓN CLAVE: Definir la ruta completa a la carpeta de Descargas
DOWNLOADS_PATH = os.path.join(os.path.expanduser('~'), 'Downloads')
NOMBRE_ARCHIVO_SALIDA_BASE = "Cat_DiasLaborablesCapacidad.xlsx"
NOMBRE_ARCHIVO_SALIDA = os.path.join(DOWNLOADS_PATH, NOMBRE_ARCHIVO_SALIDA_BASE)

# =========================================================================
# === FUNCIÓN DE DIAGNÓSTICO ===
# =========================================================================

def diagnosticar_nombre_archivo(file_name):
    """Ejecuta el diagnóstico si el archivo no se encuentra."""
    print(f"\n==================== 🛑 ERROR DE ARCHIVO NO ENCONTRADO ====================")
    print(f"El archivo '{file_name}' no se encontró en la ruta actual: {os.getcwd()}")
    print("\nArchivos encontrados en esta carpeta (buscando '.xlsx'):")

    files = [f for f in os.listdir('.') if 'power bi' in f.lower() and (f.endswith('.xlsx') or f.endswith('.xls'))]

    if files:
        print("-------------------------------------------------------------------------")
        print("💡 SOLUCIÓN: El nombre exacto de su archivo DEBE ser uno de estos:")
        for f in files:
            print(f"-> {f}")
        print(f"Por favor, verifique que la HOJA '{SHEET_NAME}' exista.")
        print("-------------------------------------------------------------------------")
    else:
        print("⚠️ ¡No se encontró ningún archivo de Excel relevante en la carpeta actual!")

# =========================================================================
# === PROCESAMIENTO PRINCIPAL ===
# =========================================================================
try:
    # 1. Lectura del EXCEL (.xlsx)
    df = pd.read_excel(
        FILE_NAME, 
        sheet_name=SHEET_NAME, 
        header=None,         # No usar ninguna fila como encabezado
        skiprows=SKIP_ROWS_COUNT # Saltar hasta la fila 18 (índice 17)
    )
    
    # 2. Limpieza y mapeo de columnas
    
    # 2a. ELIMINAR LA PRIMERA COLUMNA (ÍNDICE 0) en blanco
    if df.shape[1] > 0:
        df = df.drop(df.columns[0], axis=1)
    
    # Renombrar las columnas usando sus índices posicionales
    df = df.rename(columns={
        IDX_CAPACIDAD: NOMBRE_COLUMNA_CONCEPTO_FINAL,
        IDX_DIAS: NOMBRE_COLUMNA_DIAS_FINAL,
        IDX_FLAG: 'FLAG_FILTER_COL' # Renombramos la columna FLAG para el filtro
    })

    # 3. Filtrado (FLAG = 2)
    COL_FLAG_RENAMED = 'FLAG_FILTER_COL'
    
    if COL_FLAG_RENAMED not in df.columns:
        raise KeyError(f"La columna de filtro con índice {IDX_FLAG + 1} no fue mapeada correctamente.")

    df = df.dropna(subset=[COL_FLAG_RENAMED]).copy()
    df[COL_FLAG_RENAMED] = pd.to_numeric(df[COL_FLAG_RENAMED], errors='coerce') 
    
    df_filtered = df[df[COL_FLAG_RENAMED] == VALOR_FILTRO].copy()

    # --- Post-Filtro y Exportación ---
    if df_filtered.empty:
        raise ValueError(f"El filtro '{COL_FLAG_RENAMED} = {VALOR_FILTRO}' resultó en un DataFrame vacío.")
    
    # 4. Selección final y Limpieza
    df_resultado = df_filtered[[NOMBRE_COLUMNA_CONCEPTO_FINAL, NOMBRE_COLUMNA_DIAS_FINAL]].copy()

    df_resultado[NOMBRE_COLUMNA_CONCEPTO_FINAL] = df_resultado[NOMBRE_COLUMNA_CONCEPTO_FINAL].astype(str).str.strip()
    df_resultado[NOMBRE_COLUMNA_DIAS_FINAL] = df_resultado[NOMBRE_COLUMNA_DIAS_FINAL].astype(str).str.strip()

    # 5. Exportación a la carpeta de DESCARGAS
    df_resultado.to_excel(NOMBRE_ARCHIVO_SALIDA, index=False)
    
    # ==============================================================================

    print(f"\n================ EXPORTACIÓN COMPLETADA ================")
    print(f"✔️ Tabla exportada exitosamente a: {NOMBRE_ARCHIVO_SALIDA}")
    print(f"Filas exportadas: {len(df_resultado)}")
    print("\nEl catálogo de capacidad ajustado a dos columnas:")
    print(df_resultado.to_markdown(index=False, numalign="left", stralign="left"))

except FileNotFoundError:
    diagnosticar_nombre_archivo(FILE_NAME)
except KeyError as e:
    print(f"\n🛑 ERROR CRÍTICO DE COLUMNA: {e}.")
    print(f"Los índices posicionales son incorrectos. Verifique que las columnas de datos inician en la Fila 18.")
except ValueError as e:
    print(f"\n🛑 ERROR CRÍTICO: {e}")
    print("⚠️ El filtro de datos está funcionando, pero no encontró ninguna fila con FLAG=2. El índice de las columnas de datos puede ser incorrecto.")
except Exception as e:
    print(f"\n🛑 ERROR INESPERADO: {e}")