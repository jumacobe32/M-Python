import pandas as pd
import io
import requests
from pathlib import Path

# --- CONFIGURACIÓN ---
# ⚠️ IMPORTANTE: URL de exportación CSV para la hoja deseada.
# Si el GID=0 corresponde a la hoja "NUEVO DESEMPEÑO", usa este formato:
GOOGLE_SHEETS_EXPORT_URL = "https://docs.google.com/spreadsheets/d/1EK96qUKEW2dfnRBT7NfeVouAFouUXDOvHRVVGJ8gs34/edit?pli=1&gid=1118832498#gid=1118832498&range=10:19"
OUTPUT_FILENAME = 'ConceptosProdFlag.xlsx'

# Mapeo de columnas M a índices de Pandas (0-indexado)
# M: Column2  -> Pandas: Columna B (Índice 1)
# M: Column6  -> Pandas: Columna F (Índice 5)
# M: Column9  -> Pandas: Columna I (Índice 8)
COLUMNA_FILTRO_INDEX = 8  # Columna I (M: Column9)
COLUMNAS_SELECCIONADAS_INDEX = [1, 5, 8] # B, F, I (Índices 1, 5, 8)
COLUMNAS_FINALES_NOMBRES = ["Column2", "Column6", "Column9"]


def extraer_y_transformar_desempeno() -> pd.DataFrame:
    """Traduce la lógica M (lectura sin encabezados, filtrado por Column9 = 1) a Pandas."""
    
    print("Iniciando extracción y transformación de 'NUEVO DESEMPEÑO'...")
    
    try:
        response = requests.get(GOOGLE_SHEETS_EXPORT_URL)
        response.raise_for_status()
        
        # 1. Origen y #"DESEMPEÑO 360_Table"
        # Leemos sin encabezados (header=None) para replicar el comportamiento inicial de M
        df = pd.read_csv(
            io.StringIO(response.text), 
            header=None,  
            engine='python',
            on_bad_lines='skip'
        )
        
        # Asignar nombres a las columnas basándose en los índices para el filtrado/selección
        df.columns = [f'Column{i+1}' for i in range(len(df.columns))]
        
    except Exception as e:
        print(f"❌ Error crítico de conexión o lectura: {e}")
        return pd.DataFrame()

    # 2. #"Filas filtradas" = Table.SelectRows(..., each ([Column9] = 1))
    
    # Asegurar que la columna de filtro exista
    columna_filtro_nombre = f'Column{COLUMNA_FILTRO_INDEX + 1}'
    if columna_filtro_nombre not in df.columns:
        print(f"❌ ERROR: La columna de filtro '{columna_filtro_nombre}' (Índice {COLUMNA_FILTRO_INDEX}) no fue encontrada.")
        return pd.DataFrame()

    # Convertir la columna de filtro a string/numérico y aplicar el filtro
    try:
        # Intentamos convertir a string, limpiamos, y luego filtramos
        df_filtrado = df[
            df[columna_filtro_nombre].astype(str).str.strip() == '1'
        ].copy()
    except Exception as e:
        print(f"❌ ERROR al filtrar: No se pudo comparar '{columna_filtro_nombre}' con '1'. {e}")
        return pd.DataFrame()
    
    # 3. #"Otras columnas quitadas" = Table.SelectColumns(...)
    
    # Crear la lista de nombres de columna finales (ej: ['Column2', 'Column6', 'Column9'])
    columnas_a_mantener = [f'Column{i + 1}' for i in COLUMNAS_SELECCIONADAS_INDEX]
    
    # Filtrar solo las columnas que existen
    columnas_presentes = [col for col in columnas_a_mantener if col in df_filtrado.columns]
    
    df_final = df_filtrado[columnas_presentes]
    
    # Reasignar nombres finales si el número de columnas coincide
    if len(df_final.columns) == len(COLUMNAS_FINALES_NOMBRES):
        df_final.columns = COLUMNAS_FINALES_NOMBRES
    
    print("✔️ Transformación completada.")
    return df_final


# ----------------------------------------------------------------------------------
##  EJECUCIÓN Y EXPORTACIÓN
# ----------------------------------------------------------------------------------

if __name__ == '__main__':
    
    df_final = extraer_y_transformar_desempeno()
    
    if df_final.empty:
        print("\n🛑 El DataFrame final está vacío. Finalizando.")
        exit()
    
    # EXPORTACIÓN DEL RESULTADO
    try:
        descargas_dir = Path.home() / 'Downloads'
        descargas_dir.mkdir(parents=True, exist_ok=True) 
        output_path = descargas_dir / OUTPUT_FILENAME

        df_final.to_excel(output_path, index=False)
        
        print("\n================ EXPORTACIÓN ================")
        print(f"✅ Exportación exitosa a Excel. Archivo guardado en: {output_path}")

    except Exception as e:
        print(f"❌ Error al exportar el archivo: {e}")
        
    # Resultado final en pantalla
    print("\n================ RESULTADO FINAL ================")
    print(f"Filas procesadas: {len(df_final)}")
    print("Primeras 5 filas:")
    print(df_final.head())