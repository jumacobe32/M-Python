import pandas as pd
import os
import numpy as np

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVOS DE ORIGEN
# ==============================================================================

RUTA_BASE = r"C:\Users\USUARIO\Downloads"

# Archivo de Origen (Fact Table)
ARCHIVO_FACT = "Ext_Atencion a clientes.xlsx"
RUTA_FACT = os.path.join(RUTA_BASE, ARCHIVO_FACT)

# Archivos de Dimensión 
ARCHIVO_DIM_CLIENTE = "DimCliente.xlsx" 
ARCHIVO_DIM_EMPLEADO = "DimEmpleado.xlsx" 
ARCHIVO_DIM_PLANTA_CTE = "DimPlantaClientes.xlsx" 

RUTA_DIM_CLIENTE = os.path.join(RUTA_BASE, ARCHIVO_DIM_CLIENTE)
RUTA_DIM_EMPLEADO = os.path.join(RUTA_BASE, ARCHIVO_DIM_EMPLEADO)
RUTA_DIM_PLANTA_CTE = os.path.join(RUTA_BASE, ARCHIVO_DIM_PLANTA_CTE)

# Archivo de salida solicitado
OUTPUT_FILE_NAME = "fctAtencionClientes.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)


def safe_load_excel(file_path: str, name: str) -> pd.DataFrame:
    """Carga un archivo Excel de forma segura, intentando forzar 'Fecha' a datetime."""
    try:
        # Intentamos cargar y forzar la columna 'Fecha' a datetime para la normalización.
        df = pd.read_excel(file_path, engine='openpyxl', parse_dates=['Fecha', 'Fecha Inicio', 'Fecha Atendido'])
        print(f"✔️ Archivo '{name}' cargado.")
        return df
    except FileNotFoundError:
        print(f"❌ ERROR: El archivo '{name}' no se encontró en: {file_path}")
        return None
    except Exception as e:
        # Fallback a carga simple si parse_dates falla
        try:
             df = pd.read_excel(file_path, engine='openpyxl')
             print(f"⚠️ Aviso: Archivo '{name}' cargado sin forzar fechas.")
             return df
        except:
             print(f"❌ ERROR al cargar '{name}' (Fallo total): {e}")
             return None


def transformar_e_integrar_atencion_clientes() -> pd.DataFrame:
    """
    Aplica la lógica M para realizar los tres LEFT JOINS e integrar las claves de dimensiones.
    """
    # 1. Carga de datos
    df_fact = safe_load_excel(RUTA_FACT, "Ext_Atencion a clientes")
    df_dim_cliente = safe_load_excel(RUTA_DIM_CLIENTE, "DimCliente")
    df_dim_empleado = safe_load_excel(RUTA_DIM_EMPLEADO, "DimEmpleado")
    df_dim_planta_cte = safe_load_excel(RUTA_DIM_PLANTA_CTE, "DimPlantaClientes")

    if df_fact is None or df_dim_cliente is None or df_dim_empleado is None or df_dim_planta_cte is None:
        return pd.DataFrame()

    df_trabajo = df_fact.copy()
    
    # ==========================================================================
    # PASO DE NORMALIZACIÓN DE FECHAS (QUITAR HORAS)
    # ==========================================================================
    for col in ['Fecha', 'Fecha Inicio', 'Fecha Atendido']:
        if col in df_trabajo.columns:
            # Forzar conversión a datetime (por si acaso) y luego normalizar (quitar horas)
            df_trabajo[col] = pd.to_datetime(df_trabajo[col], errors='coerce').dt.normalize()
            print(f"✔️ Columna '{col}' normalizada (horas quitadas).")


    # --- Joins e Integración de Claves (Mapeo de LEFT OUTER JOIN a pd.merge) ---

    # 2. y 3. JOIN con DimCliente (on Cliente)
    df_trabajo = pd.merge(
        df_trabajo,
        df_dim_cliente[['Cliente', 'Key_Cliente']],
        on='Cliente',
        how='left' # JoinKind.LeftOuter
    )

    # 4. y 5. JOIN con DimEmpleado (on Empleado)
    df_trabajo = pd.merge(
        df_trabajo,
        df_dim_empleado[['Empleado', 'Key_Empleado']],
        on='Empleado',
        how='left' # JoinKind.LeftOuter
    )

    # 6. y 7. JOIN con DimPlantaClientes (on planta)
    df_trabajo = pd.merge(
        df_trabajo,
        df_dim_planta_cte[['planta', 'Key_PlantasCte']],
        on='planta',
        how='left' # JoinKind.LeftOuter
    )
    
    # 8. #"Columnas quitadas": Eliminar columnas de texto
    columnas_a_quitar = ["planta", "Cliente", "Teléfono", "Empleado"]
    df_trabajo.drop(columns=columnas_a_quitar, inplace=True, errors='ignore')

    # 9. #"Columnas reordenadas" (Asegurando que todas existan)
    columnas_ordenadas = [
        "Fecha", "Key_PlantasCte", "Key_Empleado", "Key_Cliente", "Categoría", "Clasificación", 
        "Comentario", "Calificación", "Orden de Venta", "Folio", "Status", 
        "Fecha Inicio", "Fecha Atendido", "Hora de Inicio", "Hora de Fin", 
        "Tiempo Atencion Minutos", "IndiceAtencionClientes"
    ]
    
    # Filtrar solo las columnas que están presentes en el DataFrame final
    columnas_finales = [col for col in columnas_ordenadas if col in df_trabajo.columns]
    
    # Asegurar que las columnas restantes (si las hay) no se pierdan, aunque la solicitud M las omite
    columnas_faltantes_en_orden = [col for col in df_trabajo.columns if col not in columnas_finales]
    
    df_final = df_trabajo[columnas_finales + columnas_faltantes_en_orden]

    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    print("Iniciando la integración de la tabla de hechos de Atención a Clientes...")
    
    df_resultado = transformar_e_integrar_atencion_clientes()

    if not df_resultado.empty:
        print("\n✔️ Integración completada. La tabla de hechos ahora usa claves numéricas.")
        
        # 🚀 EXPORTACIÓN A EXCEL EN CARPETA DE DESCARGAS
        try:
            # Creamos un ExcelWriter para aplicar el formato de fecha
            writer = pd.ExcelWriter(
                OUTPUT_PATH, 
                engine='openpyxl',
                # ✅ FIX: Especificar el formato de fecha sin hora/ceros
                datetime_format='yyyy-mm-dd' # Formato limpio
            )
            
            df_resultado.to_excel(writer, index=False, sheet_name='fctAtencionClientes')
            writer.close() 

            print(f"\n✅ **¡Éxito!** La tabla de hechos final se ha guardado en:")
            print(f"   {OUTPUT_PATH}")

        except Exception as e:
             print(f"\n❌ ERROR al guardar la tabla de hechos: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")
    else:
        print("\n🛑 La transformación no pudo completarse debido a errores de carga o datos faltantes.")