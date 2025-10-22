import pandas as pd
import io
import requests
from pathlib import Path
import re 
import numpy as np 

# --- CONFIGURACIÓN ---
GOOGLE_SHEETS_EDIT_URL = "https://docs.google.com/spreadsheets/d/1EK96qUKEW2dfnRBT7NfeVouAFouUXDOvHRVVGJ8gs34/edit?pli=1&gid=1118832498#gid=1118832498"
OUTPUT_FILENAME = 'ConceptosProdFlag.xlsx'

# Mapeo de columnas M a índices de Pandas (0-indexado)
COLUMNA_FILTRO_INDEX = 8 # Columna I (M: Column9) -> Índice 8
COLUMNAS_SELECCIONADAS_INDEX = [1, 5, 8] # B, F, I -> Índices 1, 5, 8
COLUMNAS_FINALES_NOMBRES = ["Column2", "Column6", "Column9"]

# ***AJUSTE CLAVE CORREGIDO***: FILAS_A_SALTAR se ajusta a 9.
# La primera fila de datos relevante (1A kg, FLAG=1) es la 10ma del CSV.
FILAS_A_SALTAR = 9 

MAX_COLUMNA_INDEX = max(COLUMNAS_SELECCIONADAS_INDEX) + 1 

def obtener_url_exportacion(edit_url: str) -> str:
    """Convierte la URL de edición de Google Sheets a la URL de exportación CSV."""
    try:
        doc_id = edit_url.split('/d/')[1].split('/edit')[0]
    except IndexError:
        return ""
    
    gid = "0"
    gid_start = edit_url.find('gid=')
    if gid_start != -1:
        gid_part = edit_url[gid_start + 4:].split('&')[0].split('#')[0]
        if gid_part.isdigit():
             gid = gid_part
    else:
        fragment = edit_url.split('#')[-1]
        if fragment.startswith('gid='):
             gid_fragment_part = fragment[4:].split('&')[0]
             if gid_fragment_part.isdigit():
                 gid = gid_fragment_part
    
    export_url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}"
    print(f"URL de exportación generada: {export_url}")
    return export_url


def extraer_y_transformar_desempeno() -> pd.DataFrame:
    """Traduce la lógica M (lectura sin encabezados, filtrado por Column9 = 1) a Pandas."""
    
    GOOGLE_SHEETS_EXPORT_URL = obtener_url_exportacion(GOOGLE_SHEETS_EDIT_URL)
    if not GOOGLE_SHEETS_EXPORT_URL:
        return pd.DataFrame()
        
    print("Iniciando extracción y transformación de 'NUEVO DESEMPEÑO'...")
    
    try:
        response = requests.get(GOOGLE_SHEETS_EXPORT_URL)
        response.raise_for_status()
        
        # 1. Origen: Lectura con skiprows ajustado
        # Se añaden 10 nombres extra para capturar la fila de encabezado
        nombres_forzados = [f'Column{i+1}' for i in range(MAX_COLUMNA_INDEX + 10)] 

        df = pd.read_csv(
            io.StringIO(response.text), 
            header=None, 
            skiprows=FILAS_A_SALTAR, # <--- AJUSTE CLAVE: 9
            engine='python',
            on_bad_lines='skip',
            names=nombres_forzados
        ).iloc[:, :MAX_COLUMNA_INDEX] # Recortamos a las columnas necesarias

    except Exception as e:
        print(f"❌ Error crítico de conexión o lectura (Verifica URL y permisos): {e}")
        return pd.DataFrame()

    columna_filtro_nombre = f'Column{COLUMNA_FILTRO_INDEX + 1}'
    print(f"\n✅ PASO 1: Archivo leído, saltando {FILAS_A_SALTAR} filas. Filtro en {columna_filtro_nombre} (Columna I).")

    # 2. Aplicamos el filtro directo (SIN LIMPIEZA)
    print("⚠️ PASO 2: Aplicando filtro directo (SIN limpieza robusta).")
    try:
        # CONVERTIMOS LA COLUMNA A NUMÉRICO Y LUEGO A STRING PARA DETECTAR '1' o '1.0'
        # Usamos .astype(str) al final para la comparación de texto.
        
        # Intentamos forzar a numérico para capturar valores como '1' (entero) o '1.0' (flotante)
        # Si falla, se queda como NaN, que luego se convierte en 'nan' al usar .astype(str)
        columna_a_filtrar_str = (
            pd.to_numeric(df[columna_filtro_nombre], errors='coerce')
            .astype(str)
            .str.strip()
        )

        # ---------------- DIAGNÓSTICO PARA CONSOLA ----------------
        print("\n--- DIAGNÓSTICO DE FILTRO (Columna I - Column9) ---")
        
        # Muestra los primeros 10 valores después de la conversión a string
        df_datos_reales = columna_a_filtrar_str[~columna_a_filtrar_str.isin(['', 'nan'])].head(10).tolist()
        
        if df_datos_reales:
            print(f"Primeros 10 valores NO vacíos/NaN después de conversión: {df_datos_reales}")
            print(f"Se espera que en este rango aparezca el valor '1.0' (Numérico) o '1' (Texto).")
        else:
            print("La columna Column9 parece vacía.")
        print("-----------------------------------------------------")
        # -----------------------------------------------------
        
        # Filtro para encontrar '1' (texto de entero) o '1.0' (texto de flotante)
        df_filtrado = df[
            (columna_a_filtrar_str == '1.0') # <-- Usamos 1.0 porque pd.to_numeric lo convierte a float
        ].copy()
        
    except Exception as e:
        print(f"❌ ERROR al aplicar el filtro: {e}")
        return pd.DataFrame()
    
    # 3. Selección y Renombrado
    
    columnas_a_mantener = [f'Column{i + 1}' for i in COLUMNAS_SELECCIONADAS_INDEX]
    columnas_presentes = [col for col in columnas_a_mantener if col in df_filtrado.columns]
    
    df_final = df_filtrado[columnas_presentes]
    
    if len(df_final.columns) == len(COLUMNAS_FINALES_NOMBRES):
        df_final.columns = COLUMNAS_FINALES_NOMBRES
    
    print("✔️ Transformación completada.")
    return df_final


# ----------------------------------------------------------------------------------
##  EJECUCIÓN Y EXPORTACIÓN
# ----------------------------------------------------------------------------------

if __name__ == '__main__':
    
    df_final = extraer_y_transformar_desempeno()
    
    if df_final.empty:
        print("\n🛑 El DataFrame final está vacío. Finalizando.")
        print(f"Causas: El valor '1.0' no se encontró o el 'skiprows={FILAS_A_SALTAR}' debe ajustarse (prueba 8 o 10).")
        
        # Imprimimos el DataFrame final (vacío) para mostrar el resultado
        print("\n================ RESULTADO FINAL ================")
        print("Filas procesadas: 0")
        print(df_final.head())
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