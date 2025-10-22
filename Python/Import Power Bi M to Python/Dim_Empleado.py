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
OUTPUT_FILE_NAME = "DimEmpleado.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)

def transformar_empleados_e_indice(file_path: str) -> pd.DataFrame:
    """
    Convierte la lógica M de creación del catálogo de empleados a Pandas:
    Selecciona Empleado, quita duplicados y añade Key_Empleado.
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
    columna_requerida = "Empleado"
    
    # Aseguramos que la columna Empleado exista
    if columna_requerida not in df_origen.columns:
        print(f"❌ ERROR: La columna clave '{columna_requerida}' no se encontró en el archivo.")
        return pd.DataFrame()
        
    # 1. #"Otras columnas quitadas" = Table.SelectColumns(Origen,{"Empleado"})
    df_temp = df_origen.loc[:, [columna_requerida]].copy()
    
    # 2. #"Duplicados quitados" = Table.Distinct(#"Otras columnas quitadas")
    df_temp.drop_duplicates(inplace=True)

    # 3. y 4. Añadir y renombrar índice:
    # #"Índice agregado" y #"Columnas con nombre cambiado"
    # Se añade un índice comenzando en 1 con paso de 1, y se nombra directamente "Key_Empleado".
    df_temp.reset_index(drop=True, inplace=True)
    df_temp['Key_Empleado'] = df_temp.index + 1
    
    # 5. #"Columnas reordenadas" = Table.ReorderColumns(...)
    df_final = df_temp.loc[:, ["Key_Empleado", "Empleado"]]

    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    df_resultado = transformar_empleados_e_indice(RUTA_ORIGEN)

    if not df_resultado.empty:
        print("\n✔️ Conversión de script M a Python completada.")
        print("\n================ RESULTADO (Catálogo de Empleados Indexado) ================")
        print(f"Filas resultantes: {len(df_resultado)}")
        print(df_resultado.head().to_markdown(index=False))
        
        # 🚀 EXPORTACIÓN A EXCEL EN CARPETA DE DESCARGAS
        try:
            df_resultado.to_excel(OUTPUT_PATH, index=False, engine='openpyxl')
            print(f"\n✅ **¡Éxito!** El catálogo de empleados se ha guardado en:")
            print(f"   {OUTPUT_PATH}")
        except Exception as e:
             print(f"\n❌ ERROR al guardar el catálogo de empleados: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")
    else:
        print("\n🛑 No se pudo generar el catálogo porque el DataFrame de origen estaba vacío o falló la carga.")