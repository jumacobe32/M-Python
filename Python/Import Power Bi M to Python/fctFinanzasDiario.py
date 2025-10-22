import pandas as pd
import os
import numpy as np

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVOS DE ORIGEN
# ==============================================================================

RUTA_BASE = r"C:\Users\USUARIO\Downloads"

# Archivos de Origen (Hechos)
ARCHIVO_FACT = "TR_Datos.xlsx"
RUTA_FACT = os.path.join(RUTA_BASE, ARCHIVO_FACT)

# Archivos de Dimensión (Catálogos previamente creados)
ARCHIVO_DIM_CONCEPTO = "DimConcepto.xlsx"
ARCHIVO_DIM_PLANTA = "DimPlanta.xlsx"
RUTA_DIM_CONCEPTO = os.path.join(RUTA_BASE, ARCHIVO_DIM_CONCEPTO)
RUTA_DIM_PLANTA = os.path.join(RUTA_BASE, ARCHIVO_DIM_PLANTA)

# Archivo de salida solicitado
OUTPUT_FILE_NAME = "fctFinanzasDiario.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)


def safe_load_excel(file_path: str, name: str) -> pd.DataFrame:
    """Carga un archivo Excel de forma segura."""
    try:
        return pd.read_excel(file_path, engine='openpyxl')
    except FileNotFoundError:
        print(f"❌ ERROR: El archivo '{name}' no se encontró en: {file_path}")
        return None
    except Exception as e:
        print(f"❌ ERROR al cargar '{name}': {e}")
        return None


def transformar_e_integrar_datos(ruta_fact: str, ruta_dim_concepto: str, ruta_dim_planta: str) -> pd.DataFrame:
    """Aplica la lógica M para crear claves compuestas, realizar joins y limpieza."""

    # 1. Carga de datos
    df_fact = safe_load_excel(ruta_fact, "TR_Datos.xlsx")
    df_dim_concepto = safe_load_excel(ruta_dim_concepto, "DimConcepto")
    df_dim_planta = safe_load_excel(ruta_dim_planta, "DimPlanta")

    if df_fact is None or df_dim_concepto is None or df_dim_planta is None:
        return pd.DataFrame()

    # Preparar columnas clave (manejar 'SEGMENTO' como 'Division')
    if 'SEGMENTO' in df_fact.columns and 'Division' not in df_fact.columns:
        df_fact.rename(columns={'SEGMENTO': 'Division'}, inplace=True)

    df_trabajo = df_fact.copy()
    
    # ==========================================================================
    # ✅ PASO DE NORMALIZACIÓN DE FECHAS (QUITAR HORAS)
    # ==========================================================================
    if 'Fecha' in df_trabajo.columns:
        # Convertir a datetime y normalizar para eliminar la parte de la hora (deja solo YYYY-MM-DD)
        df_trabajo['Fecha'] = pd.to_datetime(df_trabajo['Fecha'], errors='coerce').dt.normalize()
        print("✔️ Fechas en 'Fecha' normalizadas (horas quitadas).")


    # --- Joins e Integración de Claves ---

    # 2. #"Personalizada Agregada": Crear IdConcepto
    # each Text.From([Reporte]) & "|" & Text.From([Concepto])
    df_trabajo['IdConcepto'] = df_trabajo['Reporte'].astype(str) + '|' + df_trabajo['Concepto'].astype(str)
    
    # 3. y 4. #"Consultas combinadas" y #"Se expandió Conceptos": LEFT JOIN con DimConcepto
    # Une por IdConcepto y trae Key_Conceptos
    df_trabajo = pd.merge(
        df_trabajo,
        df_dim_concepto[['IdConcepto', 'Key_Conceptos']],
        on='IdConcepto',
        how='left' # JoinKind.LeftOuter
    )

    # 5. #"Personalizada Agregada1": Crear IdPlanta
    # each [planta]&"|"&[Division]
    df_trabajo['IdPlanta'] = df_trabajo['planta'].astype(str) + '|' + df_trabajo['Division'].astype(str)

    # 6. y 7. #"Consultas combinadas1" y #"Se expandió Plantas": LEFT JOIN con DimPlanta
    # Une por IdPlanta y trae Key_Plantas
    df_trabajo = pd.merge(
        df_trabajo,
        df_dim_planta[['IdPlanta', 'Key_Plantas']],
        on='IdPlanta',
        how='left' # JoinKind.LeftOuter
    )
    
    # 8. #"Columnas quitadas": Eliminar columnas de texto y claves temporales
    columnas_a_quitar = ["planta", "Concepto", "IdConcepto", "IdPlanta", "Reporte", "Division"]
    df_trabajo.drop(columns=columnas_a_quitar, inplace=True, errors='ignore')

    # 9. #"Índice agregado": Crear fctIndice
    # Table.AddIndexColumn(..., "fctIndice", 1, 1, Int64.Type)
    df_trabajo.reset_index(drop=True, inplace=True)
    df_trabajo['fctIndice'] = df_trabajo.index + 1
    
    # 10. #"Columnas con nombre cambiado": Renombrar Meta a Proyectado
    df_trabajo.rename(columns={"Meta": "Proyectado"}, inplace=True)
    
    # Mover las claves al inicio para una Fact Table estándar
    claves_integradas = ["fctIndice", "Key_Conceptos", "Key_Plantas"]
    otras_columnas = [col for col in df_trabajo.columns if col not in claves_integradas]
    
    df_final = df_trabajo[claves_integradas + otras_columnas]


    return df_final


# ==============================================================================
# EJECUCIÓN PRINCIPAL Y EXPORTACIÓN
# ==============================================================================

if __name__ == '__main__':
    print("Iniciando la integración de la tabla de hechos con las dimensiones...")
    
    df_resultado = transformar_e_integrar_datos(RUTA_FACT, RUTA_DIM_CONCEPTO, RUTA_DIM_PLANTA)

    if not df_resultado.empty:
        print("\n✔️ Integración completada. La tabla de hechos ahora usa claves numéricas.")
        print("\n================ RESULTADO (Tabla de Hechos Integrada) ================")
        print(f"Filas resultantes: {len(df_resultado)}")
        print(df_resultado.head().to_markdown(index=False))
        
        # 🚀 EXPORTACIÓN A EXCEL EN CARPETA DE DESCARGAS
        try:
            df_resultado.to_excel(OUTPUT_PATH, index=False, engine='openpyxl') 
            print(f"\n✅ **¡Éxito!** La tabla de hechos final se ha guardado en:")
            print(f"   {OUTPUT_PATH}")
        except Exception as e:
             print(f"\n❌ ERROR al guardar la tabla de hechos: {e}")
             print("Verifica si el archivo está abierto o si hay problemas de permisos.")
    else:
        print("\n🛑 La transformación no pudo completarse debido a errores de carga o datos faltantes.")