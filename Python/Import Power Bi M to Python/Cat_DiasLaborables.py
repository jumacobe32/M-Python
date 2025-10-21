import pandas as pd

import sys

from typing import List, Dict, Any, Optional

import numpy as np

# --- CONFIGURACIÓN AJUSTADA ---
# RUTA DEL NUEVO ARCHIVO EXCEL DE ORIGEN

GOOGLE_SHEETS_FILE_PATH = "C:\\Users\\USUARIO\\Documents\\Juan Manuel Cortes Benitez\\Python\\Import Power Bi M to Python\\Datos de power bi (1).xlsx"
# NOMBRE DE LA PESTAÑA A CARGAR
SOURCE_SHEET_NAME = "NUEVO DESEMPEÑO"

# Rutas locales del Catálogo y Exportación (sin cambios)

CATALOG_FILE_PATH = "C:/Users/USUARIO/Downloads/Cat_DiasLaborablesCapacidad.xlsx"
EXPORT_FILE_PATH = "C:/Users/USUARIO/Downloads/Cat_DiasLaborables.xlsx" 

ID_COL_NAME = "CONCEPTO"
DIAS_LABORABLES_COL = "DIAS LABORABLES"
# --- MAPPING DE COLUMNAS (B y L) ---

# B (2da columna) se mapea a Col_1 (0-indexed).

# L (12ava columna) se mapea a Col_11 (0-indexed).

CONCEPT_COL_INDEX = 1 # Mapea a la columna B (índice 1)

VALUE_COL_INDEX = 11  # Mapea a la columna L (índice 11)

# ------------------------------
## PARTE 1: FUNCIÓN DE CARGA DE DATOS (Adaptada a Excel Local con Pestaña)

def load_excel_data(file_path: str, sheet_name: str) -> pd.DataFrame:

    """Carga datos desde un archivo Excel local, especificando la pestaña."""

    print(f"Intentando cargar datos desde el archivo Excel: {file_path}")

    print(f"Usando la pestaña: {sheet_name}")
    try:
        # Cargamos el archivo, forzando a que no use encabezados para el mapeo por índice
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        print("✔️ Carga de Excel exitosa.")
        # Asignamos nombres de columna temporales para poder indexar
        df.columns = [f'Col_{i}' for i in range(df.shape[1])]
        return df
    except FileNotFoundError:
        print(f"❌ Error: Archivo no encontrado en la ruta: {file_path}.")
        return pd.DataFrame()
    except ValueError as e:
        if "Worksheet named" in str(e):
             print(f"❌ Error: La pestaña '{sheet_name}' no se encontró en el archivo.")
        else:
             print(f"❌ Error al cargar la pestaña: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error general al cargar el archivo Excel: {e}")
        return pd.DataFrame() 
## PARTE 2: TRANSFORMACIONES DE DATOS (M code Replicado)

def aplicar_transformaciones_m(df_raw: pd.DataFrame, df_catalogo: pd.DataFrame) -> pd.DataFrame:

    # 1. Pre-procesamiento del catálogo 

    if not df_catalogo.empty:

        df_catalogo.columns = [ID_COL_NAME, DIAS_LABORABLES_COL]
    else:
        print("Advertencia: El catálogo Excel estaba vacío o no se encontró.")
    if df_raw.empty:

        print("❌ DataFrame de Origen está vacío. Devolviendo solo el Catálogo.")
        return df_catalogo
    else:
        # 1. #"Otras columnas quitadas" = Table.SelectColumns({"Column2", "Column12"})
        try:

            # Seleccionamos las columnas B (índice 1) y L (índice 11)

            df_selected = df_raw.iloc[:, [CONCEPT_COL_INDEX, VALUE_COL_INDEX]].copy()

            df_selected.columns = ["Column_B", "Column_L"] 

        except IndexError:

            print(f"❌ Error: El origen no tiene las columnas en índice {CONCEPT_COL_INDEX} o {VALUE_COL_INDEX}.")

            return df_catalogo

            

        # 2. #"Filas filtradas" = Table.SelectRows(..., each ([Column12] <> "" and [Column12] <> "-"))

        df_filtered = df_selected[

            (df_selected["Column_L"].astype(str).str.strip() != "") &

            (df_selected["Column_L"].astype(str).str.strip() != "-") &

            # Filtramos la fila de encabezado (asumiendo que contiene 'CONCEPTO')

            (df_selected["Column_B"].astype(str).str.strip().str.upper() != 'CONCEPTO')

        ].copy()

        

        # 3. #"Encabezados promovidos" = Table.PromoteHeaders(...) -> Lógica simplificada

        if not df_filtered.empty:

            # Eliminamos la primera fila (el encabezado) y asignamos los nombres finales

            df_promoted = df_filtered.iloc[1:].copy()

            df_promoted.columns = [ID_COL_NAME, DIAS_LABORABLES_COL]

            df_promoted.reset_index(drop=True, inplace=True)

        else:

            print("❌ El DataFrame filtrado de Origen está vacío después del filtro inicial.")

            df_promoted = pd.DataFrame(columns=[ID_COL_NAME, DIAS_LABORABLES_COL])



        # 4. #"Consulta anexada" = Table.Combine({#"Encabezados promovidos", Cat_DiasLaborablesCapacidad})

        df_combinado = pd.concat([df_promoted, df_catalogo], ignore_index=True)

    

    # 5. Limpieza final y desduplicación 

    if not df_combinado.empty:

        df_combinado.drop_duplicates(subset=[ID_COL_NAME], keep='first', inplace=True)

        df_combinado = df_combinado[df_combinado[ID_COL_NAME].astype(str).str.strip() != ''].copy()

        df_combinado.reset_index(drop=True, inplace=True)

    

    return df_combinado



## PARTE 3: FORMATO FINAL Y EXPORTACIÓN

def aplicar_unpivot_y_exportar(df_combinado: pd.DataFrame) -> pd.DataFrame:

    df_final = df_combinado.copy()

    df_final.rename(columns={ID_COL_NAME: 'CONCEPTO', DIAS_LABORABLES_COL: 'DIAS LABORABLES'}, inplace=True)

    df_final = df_final[['CONCEPTO', 'DIAS LABORABLES']].copy()

    return df_final



## FUNCIONES AUXILIARES (load_excel_catalog)

def load_excel_catalog(file_path: str, expected_cols: List[str]) -> pd.DataFrame:

    """Carga y valida el DataFrame de Catálogo desde un archivo Excel local."""

    print(f"\n[CARGA DE CATÁLOGO EXCEL]")

    try:

        df = pd.read_excel(file_path, header=None, usecols=[0, 1])

        print(f"Dimensiones del catálogo Excel cargado: {df.shape}")

        if df.shape[1] == 2:

            df.columns = expected_cols

            df = df.apply(lambda x: x.astype(str).str.strip()).replace('', pd.NA).dropna(how='all')

            return df

        else:

            print(f"❌ Error: El archivo de catálogo Excel tiene {df.shape[1]} columnas, se esperaban 2.")

            return pd.DataFrame()

    except FileNotFoundError:

        print(f"❌ Error: Archivo de catálogo no encontrado en la ruta: {file_path}.")

        return pd.DataFrame()

    except Exception as e:

        print(f"❌ Error al cargar el archivo Excel: {e}")

        return pd.DataFrame()





if __name__ == '__main__':

    

    # 1. Cargar el Origen del archivo Excel local, especificando la pestaña

    df_desempeno_table_raw = load_excel_data(GOOGLE_SHEETS_FILE_PATH, SOURCE_SHEET_NAME)

    

    print(f"\n[DIAGNÓSTICO INICIAL - ORIGEN EXCEL]")

    print(f"Dimensiones de la tabla cruda: {df_desempeno_table_raw.shape}")

    print("-----------------------------------------")

    

    # 2. Cargar el Catálogo Excel

    df_cat_laborales = load_excel_catalog(CATALOG_FILE_PATH, ['Temp_Col_1', 'Temp_Col_2'])



    if df_desempeno_table_raw.empty and df_cat_laborales.empty:

        print("Fallo en la carga de ambos orígenes. Proceso detenido.")

        sys.exit(1)

        

    print("\n--- PASO 1: APLICANDO TRANSFORMACIONES DE POWER QUERY (M) SIMPLIFICADA ---")

    

    # 3. Aplicar las transformaciones M

    df_combinado = aplicar_transformaciones_m(df_desempeno_table_raw, df_cat_laborales)

    

    if df_combinado.empty:

        print("Fallo durante las transformaciones. Proceso detenido.")

        sys.exit(1)



    print(f"✔️ Concatenación y filtrado exitoso. Total de registros finales: {len(df_combinado)}")



    # 4. Aplicar Formato Final

    print("\n--- PASO 2: APLICANDO FORMATO FINAL (2 COLUMNAS) ---")

    df_final_unificada = aplicar_unpivot_y_exportar(df_combinado)

    

    # Imprimimos el resultado final

    print("\n================ RESULTADO FINAL ================ ")

    # 💡 CORRECCIÓN 1: Se elimina header=False para que muestre los encabezados en consola.

    print(df_final_unificada.to_string(index=False)) 

    print(f"\nTotal de filas finales: {len(df_final_unificada)}")

    print("================================================= ")

    # 5. Exportar el resultado final a Excel

    print(f"\n--- PASO 3: EXPORTANDO A EXCEL ---")
    try:
        # 💡 CORRECCIÓN 2: Se cambia header=False a header=True para incluir los encabezados en el Excel.
        df_final_unificada.to_excel(EXPORT_FILE_PATH, index=False, header=True)
        print(f"✔️ Exportación exitosa a: {EXPORT_FILE_PATH} (Con Encabezados)")
    except Exception as e:
        print(f"❌ Error al exportar a Excel: {e}")
        print("Asegúrese de que la ruta de exportación es válida y que el archivo no está abierto.")