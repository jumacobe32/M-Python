import pandas as pd
import os

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVO DE ORIGEN
# ==============================================================================

RUTA_BASE = r"C:\Users\USUARIO\Downloads"
# Usamos 'Ext_Datos.csv' como el archivo de origen (TR_Datos)
ARCHIVO_ORIGEN = "Ext_Datos.csv"
RUTA_ORIGEN = os.path.join(RUTA_BASE, ARCHIVO_ORIGEN)

def transformar_plantas_e_indice(file_path: str) -> pd.DataFrame:
    """
    Convierte la lógica M de extracción de plantas/divisiones a Pandas.

    Pasos M convertidos:
    1. Columnas quitadas (Table.SelectColumns)
    2. Duplicados quitados (Table.Distinct)
    3. Personalizada agregada (Table.AddColumn - IdPlanta)
    4. Índice agregado (Table.AddIndexColumn - Key_Plantas)
    """
    try:
        # Cargar el archivo de origen.
        # Asumiendo que Ext_Datos.csv es el formato más reciente que has usado.
        df_origen = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        print(f"❌ ERROR: El archivo de origen TR_Datos ({ARCHIVO_ORIGEN}) no se encontró en: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERROR al cargar el archivo de origen: {e}")
        return pd.DataFrame()

    # 1. #"Columnas quitadas" = Table.SelectColumns(Origen, {"planta","Division"})
    # Aseguramos el nombre de las columnas, ya que 'SEGMENTO' se usó anteriormente.
    columnas_a_mantener = ['planta', 'Division']
    
    # Manejar el caso de que "Division" pueda estar como "SEGMENTO" en el archivo CSV
    if 'SEGMENTO' in df_origen.columns and 'Division' not in df_origen.columns:
        df_origen.rename(columns={'SEGMENTO': 'Division'}, inplace=True)
        
    df_temp = df_origen[columnas_a_mantener].copy()

    # 2. #"Duplicados quitados" = Table.Distinct(#"Columnas quitadas")
    df_temp.drop_duplicates(inplace=True)

    # 3. #"Personalizada agregada" = Table.AddColumn(..., "IdPlanta", each [planta]&"|"&[Division])
    df_temp['IdPlanta'] = df_temp['planta'].astype(str) + '|' + df_temp['Division'].astype(str)

    # 4. #"Índice agregado" = Table.AddIndexColumn(..., "Key_Plantas", 1, 1, Int64.Type)
    # Se añade un índice comenzando en 1 con paso de 1, como en M.
    df_temp.reset_index(drop=True, inplace=True)
    df_temp['Key_Plantas'] = df_temp.index + 1
    
    # Reordenar las columnas al estilo M (clave al final)
    df_final = df_temp[['planta', 'Division', 'IdPlanta', 'Key_Plantas']]

    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================

if __name__ == '__main__':
    df_resultado = transformar_plantas_e_indice(RUTA_ORIGEN)

    if not df_resultado.empty:
        print("✔️ Conversión de script M a Python completada.")
        print("\n================ RESULTADO (Catálogo de Plantas) ================")
        print(df_resultado.to_markdown(index=False))
        
        # Opcional: Exportar este catálogo si es necesario (ejemplo: para otro merge)
        OUTPUT_PLANTAS_PATH = os.path.join(RUTA_BASE, "DimPlanta.xlsx")
        try:
            df_resultado.to_excel(OUTPUT_PLANTAS_PATH, index=False, engine='openpyxl')
            print(f"\n✅ Catálogo de Plantas guardado en:")
            print(f"   {OUTPUT_PLANTAS_PATH}")
        except Exception as e:
             print(f"\n❌ ERROR al guardar el catálogo de plantas: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")