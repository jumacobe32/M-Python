import pandas as pd
import numpy as np
import os

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVO DE ORIGEN
# ==============================================================================

RUTA_BASE = r"C:\Users\USUARIO\Downloads"

# Archivos de origen (el origen M, TR_Datos, será ahora TR_Datos.xlsx)
ARCHIVO_ORIGEN_TR_DATOS = "TR_Datos.xlsx"
RUTA_ORIGEN = os.path.join(RUTA_BASE, ARCHIVO_ORIGEN_TR_DATOS)

# Archivo de salida solicitado
OUTPUT_FILE_NAME = "DimConcepto.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)

def transformar_conceptos_e_indice(file_path: str) -> pd.DataFrame:
    """
    Convierte la lógica M de creación del catálogo de conceptos a Pandas.
    La tabla de origen es el archivo TR_Datos.xlsx.
    """
    try:
        # Cargar el archivo de origen (Excel)
        df_origen = pd.read_excel(file_path, engine='openpyxl')
        print(f"Archivo de origen '{ARCHIVO_ORIGEN_TR_DATOS}' cargado.")
    except FileNotFoundError:
        print(f"❌ ERROR: El archivo de origen TR_Datos.xlsx no se encontró en: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERROR al cargar el archivo de origen: {e}")
        return pd.DataFrame()

    # Columnas requeridas del script M
    columnas_a_mantener = ["Concepto", "Reporte", "Orden", "Unidad", "Concepto2"]
    
    # Manejar el caso de que la columna "Concepto2" esté nombrada diferente
    if 'CONCEPTO 2' in df_origen.columns:
        df_origen.rename(columns={'CONCEPTO 2': 'Concepto2'}, inplace=True)
    if 'Concepto 2' in df_origen.columns:
        df_origen.rename(columns={'Concepto 2': 'Concepto2'}, inplace=True)
        
    # Aseguramos que solo trabajamos con las columnas que existen
    cols_presentes = [col for col in columnas_a_mantener if col in df_origen.columns]
    
    if len(cols_presentes) < 5:
        print(f"❌ ERROR: Faltan columnas clave en el origen. Columnas esperadas: {columnas_a_mantener}")
        return pd.DataFrame()
        
    # 1. #"Columnas quitadas"
    df_temp = df_origen.loc[:, cols_presentes].copy()
    
    # Normalizamos (buena práctica antes de crear la clave compuesta)
    df_temp['Concepto'] = df_temp['Concepto'].astype(str).str.strip()
    df_temp['Reporte'] = df_temp['Reporte'].astype(str).str.strip()


    # 2. #"Personalizada agregada": Crear IdConcepto (Clave Compuesta)
    # IdConcepto = Text.From([Reporte]) & "|" & Text.From([Concepto])
    df_temp['IdConcepto'] = df_temp['Reporte'] + '|' + df_temp['Concepto']

    # 3. quitarDuplicados = Table.Distinct(#"Personalizada agregada")
    # Quitamos duplicados basados en todas las columnas existentes hasta este punto
    df_temp.drop_duplicates(inplace=True)

    # 4. #"Índice agregado": Crear Key_Conceptos (inicia en 1)
    # Table.AddIndexColumn(..., "Key_Conceptos", 1, 1, Int64.Type)
    df_temp.reset_index(drop=True, inplace=True)
    df_temp['Key_Conceptos'] = df_temp.index + 1
    
    # 5. #"Valor reemplazado": Reemplazar "" por null en Concepto2
    # Table.ReplaceValue(#"Índice agregado", "", null, Replacer.ReplaceValue,{"Concepto2"})
    if 'Concepto2' in df_temp.columns:
        # Reemplazar cadenas vacías con NaN (nulos)
        df_temp['Concepto2'].replace('', np.nan, inplace=True)

    # Reordenar las columnas
    columnas_finales = [
        "Concepto", "Reporte", "Orden", "Unidad", "Concepto2", "IdConcepto", "Key_Conceptos"
    ]
    df_final = df_temp[[col for col in columnas_finales if col in df_temp.columns]]

    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    df_resultado = transformar_conceptos_e_indice(RUTA_ORIGEN)

    if not df_resultado.empty:
        print("\n✔️ Conversión de script M a Python completada.")
        print("\n================ RESULTADO (Catálogo de Conceptos Indexado) ================")
        print(f"Filas resultantes: {len(df_resultado)}")
        print(df_resultado.head().to_markdown(index=False))
        
        # EXPORTACIÓN A EXCEL EN CARPETA DE DESCARGAS
        try:
            # ✅ CORRECCIÓN: 'openypxl' se cambió a 'openpyxl'
            df_resultado.to_excel(OUTPUT_PATH, index=False, engine='openpyxl') 
            print(f"\n✅ **¡Éxito!** El catálogo de conceptos se ha guardado en:")
            print(f"   {OUTPUT_PATH}")
        except Exception as e:
             print(f"\n❌ ERROR al guardar el catálogo de conceptos: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")
    else:
        print("\n🛑 No se pudo generar el catálogo porque el DataFrame de origen estaba vacío o falló la carga.")