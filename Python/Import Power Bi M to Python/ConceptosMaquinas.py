import pandas as pd
import io
import requests
import numpy as np
from pathlib import Path

# --- CONFIGURACIÓN ---
GOOGLE_SHEETS_EXPORT_URL = "https://docs.google.com/spreadsheets/d/1EK96qUKEW2dfnRBT7NfeVouAFouUXDOvHRVVGJ8gs34/gviz/tq?tqx=out:csv&gid=700246857"
COLUMNAS_FINALES_M = ["CONCEPTO", "REAL", "META", "UNIDAD", "ORDEN", "PLANTA"]
OUTPUT_FILENAME = 'ConceptosMaquinas.xlsx'

#  AJUSTAR ESTOS ÍNDICES SI ES NECESARIO 
# Posición de las columnas que DEBERÍAN CONTENER los datos REAL y META
CONCEPTO_INDEX_POS = 0 
REAL_INDEX_POS = 1     
META_INDEX_POS = 2     
ORDEN_INDEX_POS = 4    

# --- FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN FINAL ---

def extraer_y_transformar_maquinas_final() -> pd.DataFrame:
    print("Iniciando extracción y tratamiento de REAL/META como TEXTO...")
    
    try:
        response = requests.get(GOOGLE_SHEETS_EXPORT_URL)
        response.raise_for_status()
        
        # 1. Lectura del CSV sin encabezados
        df_raw = pd.read_csv(
            io.StringIO(response.text), 
            header=None, 
            engine='python',
            on_bad_lines='skip'
        )
        
    except Exception as e:
        print(f"Error crítico de conexión: {e}")
        return pd.DataFrame()

    # --- 2. PROMOCIÓN DE ENCABEZADOS Y LIMPIEZA INICIAL ---
    df_raw.columns = df_raw.iloc[0].astype(str).str.strip().str.upper()
    df = df_raw[1:].reset_index(drop=True).copy()
    df.columns = df.columns.astype(str).str.strip().str.upper()

    # 3. ASIGNACIÓN DE NOMBRES POR POSICIÓN (Mapeo)
    columnas_mapeadas = {}
    
    for i, col_name in enumerate(df.columns):
        if i == CONCEPTO_INDEX_POS:
            columnas_mapeadas[col_name] = 'CONCEPTO'
        elif i == REAL_INDEX_POS:
            columnas_mapeadas[col_name] = 'REAL_DATOS'
        elif i == META_INDEX_POS:
            columnas_mapeadas[col_name] = 'META_DATOS'
        elif i == ORDEN_INDEX_POS:
            columnas_mapeadas[col_name] = 'ORDEN_DATOS'
    
    df.rename(columns=columnas_mapeadas, inplace=True)
    
    # ----------------------------------------------------------------------
    # 4. TRATAMIENTO DE REAL Y META COMO REFERENCIAS DE TEXTO 
    # ----------------------------------------------------------------------
    
    df_filtrado = df.dropna(subset=['CONCEPTO']).copy()
    df_resultado = df_filtrado.copy()
    
    for col_temp, col_final in [('REAL_DATOS', 'REAL'), ('META_DATOS', 'META')]:
        if col_temp in df_resultado.columns:
            # Aseguramos que sea STRING y limpiamos espacios.
            df_resultado[col_final] = df_resultado[col_temp].astype(str).str.strip() 
        else:
            df_resultado[col_final] = "" 

    # Conversión de ORDEN (sigue siendo numérico)
    if 'ORDEN_DATOS' in df_resultado.columns:
        df_resultado['ORDEN'] = df_resultado['ORDEN_DATOS'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df_resultado['ORDEN'] = pd.to_numeric(df_resultado['ORDEN'], errors='coerce').astype('Int64')
    else:
        df_resultado['ORDEN'] = np.nan

    # 5. Selección Final
    df_final = df_resultado[['CONCEPTO', 'REAL', 'META', 'UNIDAD', 'ORDEN', 'PLANTA']].copy()
    
    # Convertir columnas de texto finales
    for col in ["CONCEPTO", "UNIDAD", "PLANTA", "REAL", "META"]:
        if col in df_final.columns:
            # Forzamos a string antes de rellenar los nulos.
            df_final[col] = df_final[col].astype(str)
            
            #  REEMPLAZO DE 'nan' y 'None' por cadena vacía ""
            df_final[col] = df_final[col].replace(['nan', 'None'], '', regex=False)
            
            # Limpiamos espacios extra
            df_final[col] = df_final[col].str.strip()
            
    print("\n✔️ Mapeo y transformación completados. Celdas vacías aseguradas.")
    return df_final

# ----------------------------------------------------------------------------------
## EJECUCIÓN Y EXPORTACIÓN
# ----------------------------------------------------------------------------------

if __name__ == '__main__':
    
    df_final = extraer_y_transformar_maquinas_final()
    
    if df_final.empty:
        print("\n🛑 El DataFrame final está vacío. Finalizando.")
        exit()
    
    # EXPORTACIÓN DEL RESULTADO A LA CARPETA DE DESCARGAS
    try:
        descargas_dir = Path.home() / 'Downloads'
        descargas_dir.mkdir(parents=True, exist_ok=True) 
        output_path = descargas_dir / OUTPUT_FILENAME

        # Usamos engine='xlsxwriter' por su mejor manejo de strings
        df_final.to_excel(output_path, index=False, engine='xlsxwriter')
        
        print("\n================ EXPORTACIÓN ================")
        print(f"✅ Exportación exitosa a Excel. Archivo guardado en: {output_path}")

    except Exception as e:
        print(f"❌ Error crítico al exportar el archivo: {e}")
        
    # Resultado final en pantalla
    print("\n================ RESULTADO FINAL (MAESTRO) ================")
    print(f"Filas procesadas: {len(df_final)}")
    print("Primeras 5 filas (Las celdas vacías ahora son cadenas vacías):")
    print(df_final.head())
    print("\nTipos de Columna:")
    print(df_final.dtypes)