import pandas as pd
import os

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVO DE ORIGEN
# ==============================================================================

RUTA_BASE = r"C:\Users\USUARIO\Downloads"
# Asumimos que el archivo de origen M se llama Ext_Atencion a clientes.xlsx
ARCHIVO_ORIGEN = "Ext_Atencion a clientes.xlsx" 
RUTA_ORIGEN = os.path.join(RUTA_BASE, ARCHIVO_ORIGEN)

# Archivo de salida solicitado
OUTPUT_FILE_NAME = "DimCliente.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)

def transformar_clientes_e_indice(file_path: str) -> pd.DataFrame:
    """
    Convierte la lógica M de creación del catálogo de clientes a Pandas.
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

    # Columnas requeridas del script M
    columnas_requeridas = ["Cliente", "Teléfono"]
    
    # 1. #"Otras columnas quitadas" = Table.SelectColumns(Origen,{"Cliente", "Teléfono"})
    # Aseguramos que solo trabajamos con las columnas requeridas
    cols_presentes = [col for col in columnas_requeridas if col in df_origen.columns]
    
    if len(cols_presentes) < 2:
        print(f"❌ ERROR: Faltan las columnas clave en el origen. Columnas esperadas: {columnas_requeridas}")
        return pd.DataFrame()
        
    df_temp = df_origen.loc[:, cols_presentes].copy()
    
    # 2. #"Duplicados quitados" = Table.Distinct(#"Otras columnas quitadas")
    df_temp.drop_duplicates(inplace=True)

    # 3. #"Índice agregado" = Table.AddIndexColumn(#"Duplicados quitados", "Índice", 1, 1, Int64.Type)
    # 4. #"Columnas con nombre cambiado" = Table.RenameColumns(#"Índice agregado",{{"Índice", "Key_Cliente"}})
    # Se añade un índice comenzando en 1 con paso de 1, y se nombra directamente.
    df_temp.reset_index(drop=True, inplace=True)
    df_temp['Key_Cliente'] = df_temp.index + 1
    
    # 5. #"Columnas reordenadas" = Table.ReorderColumns(...)
    df_final = df_temp.loc[:, ["Key_Cliente", "Cliente", "Teléfono"]]

    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    df_resultado = transformar_clientes_e_indice(RUTA_ORIGEN)

    if not df_resultado.empty:
        print("\n✔️ Conversión de script M a Python completada.")
        print("\n================ RESULTADO (Catálogo de Clientes Indexado) ================")
        print(f"Filas resultantes: {len(df_resultado)}")
        print(df_resultado.head().to_markdown(index=False))
        
        # 🚀 EXPORTACIÓN A EXCEL EN CARPETA DE DESCARGAS
        try:
            df_resultado.to_excel(OUTPUT_PATH, index=False, engine='openpyxl')
            print(f"\n✅ **¡Éxito!** El catálogo de clientes se ha guardado en:")
            print(f"   {OUTPUT_PATH}")
        except Exception as e:
             print(f"\n❌ ERROR al guardar el catálogo de clientes: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")
    else:
        print("\n🛑 No se pudo generar el catálogo porque el DataFrame de origen estaba vacío o falló la carga.")