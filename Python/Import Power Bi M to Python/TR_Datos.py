import pandas as pd
import numpy as np
import os
from typing import List, Dict, Any, Optional

# ==============================================================================
# CONFIGURACIÓN DE RUTAS Y COLUMNAS
# ==============================================================================
# Rutas de Archivos de Catálogo
RUTA_BASE = r"C:\Users\USUARIO\Downloads"
RUTAS_CATALOGOS = {
    'TR_Real': os.path.join(RUTA_BASE, "TR_Real.xlsx"),
    'ConceptosMaquinas': os.path.join(RUTA_BASE, "ConceptosMaquinas.xlsx"),
    'DiasFestivos': os.path.join(RUTA_BASE, "DiasFestivos.xlsx"),
    'Ext_DiasLaborados': os.path.join(RUTA_BASE, "Ext_DiasLaborados.xlsx"),
    'ConceptosProdFlag': os.path.join(RUTA_BASE, "ConceptosProdFlag.xlsx"),
}
# Nombre del archivo de salida
OUTPUT_FILE_NAME = "TR_Datos.xlsx"
OUTPUT_PATH = os.path.join(RUTA_BASE, OUTPUT_FILE_NAME)


# Columnas de los catálogos que necesitan ser normalizadas
NORMALIZACION_MAP = {
    'ConceptosMaquinas': {'keys': ['PLANTA', 'CONCEPTO']},
    'DiasFestivos': {'keys': ['Fecha'], 'date_cols': ['Fecha']},
    'ConceptosProdFlag': {'keys': ['Column2', 'Column6']},
    'Ext_DiasLaborados': {'keys': ['planta', 'Conceptos_DiasLaborados'], 'date_cols': ['date']},
}


# ==============================================================================
# FUNCIÓN DE CARGA Y PREPROCESAMIENTO
# ==============================================================================

def safe_load_and_normalize(file_path: str, catalog_name: str) -> Optional[pd.DataFrame]:
    """Carga un archivo Excel, normaliza las claves de texto y asegura tipos de fecha."""
    try:
        # Se asume que el archivo base TR_Real también es Excel, ya que los catálogos lo son.
        df = pd.read_excel(file_path, engine='openpyxl')
        config = NORMALIZACION_MAP.get(catalog_name, {})
        
        # Normalizar claves de texto
        for col in config.get('keys', []):
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.upper().str.strip()
        
        # Convertir a datetime
        for col in config.get('date_cols', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # === FIX para el KeyError: 'id' en DiasFestivos ===
        if catalog_name == 'DiasFestivos' and df is not None:
            if 'id' not in df.columns:
                df['id'] = 1
                print(f"    ✅ FIX aplicado: Columna 'id' agregada con valor 1 a {catalog_name}.")
        # ===================================================
                
        return df
    except FileNotFoundError:
        print(f"❌ ERROR: Archivo no encontrado para {catalog_name} en: {file_path}")
        return None
    except Exception as e:
        print(f"❌ ERROR al cargar o normalizar {catalog_name}: {e}")
        return None

def load_all_data() -> Dict[str, Optional[pd.DataFrame]]:
    """Carga todos los DataFrames requeridos (base y catálogos)."""
    data = {}
    print("Iniciando carga de DataFrames...")
    
    # Cargar el DataFrame base
    data['df_origen'] = safe_load_and_normalize(RUTAS_CATALOGOS['TR_Real'], 'TR_Real')
    
    # Cargar los catálogos
    data['df_conceptos_maquinas'] = safe_load_and_normalize(RUTAS_CATALOGOS['ConceptosMaquinas'], 'ConceptosMaquinas')
    data['df_dias_festivos'] = safe_load_and_normalize(RUTAS_CATALOGOS['DiasFestivos'], 'DiasFestivos')
    data['df_dias_laborados'] = safe_load_and_normalize(RUTAS_CATALOGOS['Ext_DiasLaborados'], 'Ext_DiasLaborados')
    data['df_conceptos_prod_flag'] = safe_load_and_normalize(RUTAS_CATALOGOS['ConceptosProdFlag'], 'ConceptosProdFlag')
    
    # Verificar si el DF Origen cargó correctamente
    if data['df_origen'] is not None:
        # Renombrar columnas comunes si existen (por si el encabezado de Excel varía)
        if "Concepto Reporte" in data['df_origen'].columns:
            data['df_origen'].rename(columns={"Concepto Reporte": "Concepto_Reporte"}, inplace=True)
        if "SEGMENTO" in data['df_origen'].columns:
            data['df_origen'].rename(columns={"SEGMENTO": "Division"}, inplace=True)
        
        print(f"✔️ Carga de TR_Real (df_origen) completada. {len(data['df_origen'])} filas.")

    return data


# ==============================================================================
# FUNCIÓN DE TRANSFORMACIÓN (LÓGICA M)
# ==============================================================================

def aplicar_logica_m_completa(
    df_origen: pd.DataFrame, 
    df_conceptos_maquinas: pd.DataFrame, 
    df_dias_festivos: pd.DataFrame, 
    df_conceptos_prod_flag: pd.DataFrame, 
    df_dias_laborados: pd.DataFrame
) -> pd.DataFrame:
    """Aplica la lógica compleja de transformación M."""

    df_trabajo = df_origen.copy()

    # Asegurar que las columnas clave para joins y filtros estén listas
    if 'Orden' not in df_trabajo.columns:
        print("❌ Error: Columna 'Orden' faltante. No se puede ejecutar el filtro Máquinas.")
        return pd.DataFrame()

    # Normalizar claves de df_trabajo
    df_trabajo['Concepto'] = df_trabajo['Concepto'].astype(str).str.upper().str.strip()
    df_trabajo['planta'] = df_trabajo['planta'].astype(str).str.upper().str.strip()
    df_trabajo['Concepto Capacidad'] = df_trabajo['Concepto Capacidad'].astype(str).str.upper().str.strip()
    df_trabajo['Fecha'] = pd.to_datetime(df_trabajo['Fecha'], errors='coerce')


    # ==========================================================================
    # 1. INICIA CARGA MÁQUINAS Y NO MÁQUINAS POR SEPARADO
    # ==========================================================================
    
    # 1.1. Carga registros de máquinas (Orden = 1000)
    df_maquinas = df_trabajo[df_trabajo['Orden'] == 1000].copy()
    
    # 1.2. Restricción de Máquinas por Planta (Inner Join con ConceptosMaquinas)
    df_maquinas_restringidas = pd.merge(
        df_maquinas,
        df_conceptos_maquinas,
        left_on=['planta', 'Concepto'],
        right_on=['PLANTA', 'CONCEPTO'],
        how='inner' # Aplica la restricción
    )
    
    # 1.3. Eliminar columnas del catálogo (Columnas quitadas)
    cols_to_remove = [c for c in ['PLANTA', 'CONCEPTO'] if c in df_maquinas_restringidas.columns]
    df_maquinas_restringidas.drop(columns=cols_to_remove, inplace=True, errors='ignore')
    
    # 1.4. Carga de registros no máquinas (Orden != 1000)
    df_sin_maquinas = df_trabajo[df_trabajo['Orden'] != 1000].copy()
    
    # 1.5. Concatenación de las dos tablas
    df_concatena = pd.concat([df_maquinas_restringidas, df_sin_maquinas], ignore_index=True)
    
    # ==========================================================================
    # 2. PROCESOS PARA ELIMINAR DÍAS DOMINGOS Y FESTIVOS EN LAS METAS
    # ==========================================================================
    
    # 2.1. IdSemana Agregada: (0 = Domingo)
    df_concatena['Id_Semana'] = df_concatena['Fecha'].dt.dayofweek + 1
    df_concatena['Id_Semana'] = df_concatena['Id_Semana'].replace({7: 0}) 

    # 2.2. Join DíasFestivos (Left Outer Join)
    df_concatena = pd.merge(
        df_concatena,
        df_dias_festivos[['Fecha', 'id']],
        on='Fecha',
        how='left'
    ).rename(columns={'id': 'id_Festivo'}) 
    df_concatena['id_Festivo'].fillna(0, inplace=True)
    df_concatena['id_Festivo'] = df_concatena['id_Festivo'].astype(int)

    # 2.3. Join ConceptosProdFlag (Left Outer Join)
    df_concatena = pd.merge(
        df_concatena,
        df_conceptos_prod_flag[['Column2', 'Column6', 'Column9']],
        left_on=['Concepto', 'Reporte'],
        right_on=['Column2', 'Column6'],
        how='left'
    ).rename(columns={'Column9': 'id_Conceptos_Prod'}) 
    
    df_concatena.drop(columns=['Column2', 'Column6'], inplace=True, errors='ignore')
    
    # 2.4. Columna condicional (Ajuste de Meta a 0)
    condicion_dia_inhabil = (df_concatena['Id_Semana'] == 0) | (df_concatena['id_Festivo'] == 1)
    condicion_conceptos_aplicables = (df_concatena['Reporte'] == "Desempeño 360") | \
                                     (df_concatena['id_Conceptos_Prod'] == 1) | \
                                     (df_concatena['Orden'] == 1000)
    
    condicion_final = condicion_dia_inhabil & condicion_conceptos_aplicables
    
    df_concatena['Meta_Temp'] = np.where(condicion_final, 0, df_concatena['Meta'])
    
    # 2.5. Limpieza y Renombrado
    df_concatena.drop(columns=['Meta', 'id_Festivo', 'Id_Semana', 'id_Conceptos_Prod'], inplace=True, errors='ignore')
    df_concatena.rename(columns={'Meta_Temp': 'Meta'}, inplace=True)

    # ==========================================================================
    # 3. MULTIPLICAR LAS METAS POR LOS DÍAS LABORADOS
    # ==========================================================================
    
    # 3.1. Join Días Laborados para CONCEPTOS (Left Outer Join)
    df_concatena = pd.merge(
        df_concatena,
        df_dias_laborados[['date', 'planta', 'Conceptos_DiasLaborados', 'Dias_Laborados']],
        left_on=['Fecha', 'planta', 'Concepto'],
        right_on=['date', 'planta', 'Conceptos_DiasLaborados'],
        how='left',
        suffixes=('', '_ConceptoLaborado') 
    )
    df_concatena.drop(columns=['date', 'Conceptos_DiasLaborados'], inplace=True, errors='ignore')
    df_concatena['Dias_Laborados'].fillna(1.0, inplace=True) 

    # 3.2. Join Días Laborados para CONCEPTO CAPACIDAD (Left Outer Join)
    df_concatena = pd.merge(
        df_concatena,
        df_dias_laborados[['date', 'planta', 'Conceptos_DiasLaborados', 'Dias_Laborados']],
        left_on=['Fecha', 'planta', 'Concepto Capacidad'],
        right_on=['date', 'planta', 'Conceptos_DiasLaborados'],
        how='left',
        suffixes=('_Concepto', '_Capacidad')
    ).rename(columns={'Dias_Laborados_Capacidad': 'Dias_LaboradosCapacidad'}) 

    df_concatena.drop(columns=['date_Capacidad', 'Conceptos_DiasLaborados_Capacidad'], inplace=True, errors='ignore')
    df_concatena['Dias_LaboradosCapacidad'].fillna(1.0, inplace=True) 

    # 3.3. Multiplicación
    df_concatena['ProyectadoTemp'] = df_concatena['Meta'] * df_concatena['Dias_Laborados_Concepto']
    df_concatena['CapacidadTemp'] = df_concatena['Valor Capacidad'] * df_concatena['Dias_LaboradosCapacidad']

    # 3.4. Limpieza y Renombrado
    cols_a_quitar_meta = ['Meta', 'Valor Capacidad', 'Dias_Laborados_Concepto', 'Dias_LaboradosCapacidad']
    df_concatena.drop(columns=cols_a_quitar_meta, inplace=True, errors='ignore')
    df_concatena.rename(columns={'ProyectadoTemp': 'Meta', 'CapacidadTemp': 'Valor Capacidad'}, inplace=True)

    # 3.5. Tipo cambiado
    df_concatena['Meta'] = pd.to_numeric(df_concatena['Meta'], errors='coerce')
    df_concatena['Valor Capacidad'] = pd.to_numeric(df_concatena['Valor Capacidad'], errors='coerce')

    # ==========================================================================
    # 4. FINALIZACIÓN Y REORDENAMIENTO
    # ==========================================================================
    final_cols_order = [
        "Fecha", "planta", "Division", "Reporte", "Concepto", "Unidad", "Orden", 
        "Concepto2", "Concepto Capacidad", "Real", "Meta", "Valor Tope", 
        "Valor Planta", "Valor Transito", "Valor Capacidad", "Valor Capacidad 91"
    ]
    
    cols_presentes = [col for col in final_cols_order if col in df_concatena.columns]
    
    return df_concatena[cols_presentes]


# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================

if __name__ == '__main__':
    
    data = load_all_data()
    
    df_origen = data.get('df_origen')
    df_maquinas = data.get('df_conceptos_maquinas')
    df_festivos = data.get('df_dias_festivos')
    df_flag = data.get('df_conceptos_prod_flag')
    df_laborados = data.get('df_dias_laborados')
    
    if all(df is not None for df in [df_origen, df_maquinas, df_festivos, df_flag, df_laborados]):
        print("\nTodos los DataFrames de origen y catálogos cargados correctamente. Iniciando transformación...")
        
        df_resultado = aplicar_logica_m_completa(
            df_origen, df_maquinas, df_festivos, df_flag, df_laborados
        )
        
        if not df_resultado.empty:
            print("\n================ RESULTADO FINAL DEL REPORTE ================")
            print(f"Filas resultantes: {len(df_resultado)}")
            print("\nPrimeras 5 filas:")
            print(df_resultado.head().to_markdown(index=False))
            
            # EXPORTAR A EXCEL EN CARPETA DE DESCARGAS
            try:
                df_resultado.to_excel(OUTPUT_PATH, index=False, engine='openpyxl')
                print(f"\n✅ **¡Éxito!** El archivo final se ha guardado en:")
                print(f"   {OUTPUT_PATH}")
            except Exception as e:
                print(f"\n❌ ERROR al exportar a Excel: {e}")
                print("Por favor, verifica que el archivo no esté abierto y que la ruta sea accesible.")
                
        else:
            print("\n🛑 El DataFrame final está vacío después de la transformación M.")
    else:
        print("\n🛑 Falló la carga de uno o más archivos. La transformación no pudo iniciarse.")