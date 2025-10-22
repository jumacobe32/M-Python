import pandas as pd
import os

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVO DE ORIGEN
# ==============================================================================

RUTA_BASE = r"C:\Users\USUARIO\Downloads"
# El origen M ("Ext_Atencion a clientes") se mapea al archivo Excel
ARCHIVO_ORIGEN = "Ext_Atencion a clientes.xlsx" 
RUTA_ORIGEN = os.path.join(RUTA_BASE, ARCHIVO_ORIGEN)

# Archivo de salida solicitado
OUTPUT_FILE_NAME = "DimPlantaClientes.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)

def transformar_plantas_cte_e_indice(file_path: str) -> pd.DataFrame:
    """
    Convierte la lógica M de creación del catálogo de plantas de cliente a Pandas:
    Selecciona 'planta', quita duplicados y añade Key_PlantasCte.
    """
    try:
        # Cargar el archivo de origen (Excel)
        df_origen = pd.read_excel(file_path, engine='openpyxl')
        print(f"Archivo de origen '{ARCHIVO_ORIGEN}' cargado.")
    except FileNotFoundError:
        print(f"❌ ERROR: El archivo de origen '{ARCHIVO_ORIGEN}' no se encontró en: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERROR al cargar el archivo de origen: {e}")
        return pd.DataFrame()

    # Columna requerida del script M
    columna_requerida = "planta"
    
    # Aseguramos que la columna 'planta' exista
    if columna_requerida not in df_origen.columns:
        print(f"❌ ERROR: La columna clave '{columna_requerida}' no se encontró en el archivo.")
        return pd.DataFrame()
        
    # 1. #"Otras columnas quitadas" = Table.SelectColumns(Origen,{"planta"})
    df_temp = df_origen.loc[:, [columna_requerida]].copy()
    
    # 2. #"Duplicados quitados1" = Table.Distinct(#"Otras columnas quitadas")
    df_temp.drop_duplicates(inplace=True)

    # 3. #"Índice agregado" = Table.AddIndexColumn(#"Duplicados quitados1", "Key_PlantasCte", 1, 1, Int64.Type)
    # Se añade un índice comenzando en 1 con paso de 1, y se nombra "Key_PlantasCte".
    df_temp.reset_index(drop=True, inplace=True)
    df_temp['Key_PlantasCte'] = df_temp.index + 1
    
    # Reordenar las columnas para que la clave quede al inicio
    df_final = df_temp.loc[:, ["Key_PlantasCte", "planta"]]

    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    df_resultado = transformar_plantas_cte_e_indice(RUTA_ORIGEN)

    if not df_resultado.empty:
        print("\n✔️ Conversión de script M a Python completada.")
        print("\n================ RESULTADO (Catálogo de Plantas Cliente Indexado) ================")
        print(f"Filas resultantes: {len(df_resultado)}")
        print(df_resultado.head().to_markdown(index=False))
        
        # 🚀 EXPORTACIÓN A EXCEL EN CARPETA DE DESCARGAS
        try:
            df_resultado.to_excel(OUTPUT_PATH, index=False, engine='openpyxl')
            print(f"\n✅ **¡Éxito!** El catálogo de plantas de cliente se ha guardado en:")
            print(f"   {OUTPUT_PATH}")
        except Exception as e:
             print(f"\n❌ ERROR al guardar el catálogo de plantas de cliente: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")
    else:
        print("\n🛑 No se pudo generar el catálogo porque el DataFrame de origen estaba vacío o falló la carga.")