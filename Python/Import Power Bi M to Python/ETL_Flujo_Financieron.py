import subprocess
import sys
import os
from typing import List, Dict
from datetime import datetime

# ==============================================================================
#                      CONFIGURACIÓN PRINCIPAL
# ==============================================================================

#  1. RUTA AL INTÉRPRETE DE PYTHON
PYTHON_EXECUTABLE = 'python' 

#  2. DEFINICIÓN DE RUTAS BASE Y SEGMENTOS
# -----------------------------------------------------------------------------
# Se define la ruta base para simplificar la lectura del código.
BASE_PATH = r'C:\Users\USUARIO\Documents\Juan Manuel Cortes Benitez\Python\Import Power Bi M to Python'

# Agrupación de scripts por flujo
#  FASE 1: EXTRACCIÓN Y PREPARACIÓN DE DATOS (FLUJO FINANCIERO / ETAPA DE PREP)
FINANCIERO_SCRIPTS: List[str] = [
    os.path.join(BASE_PATH, 'Ext_data.py'),
    os.path.join(BASE_PATH, 'ConceptosReporte.py'),
    os.path.join(BASE_PATH, 'ConceptoInventario.py'),
    os.path.join(BASE_PATH, 'ConceptosMaquinas.py'),
    os.path.join(BASE_PATH, 'ConceptosProdFlag.py'),
    os.path.join(BASE_PATH, 'DiasFestivos.py'),
    os.path.join(BASE_PATH, 'Cat_DiasLaborablesCapacidad.py'),
    os.path.join(BASE_PATH, 'Cat_DiasLaborables.py'),
    os.path.join(BASE_PATH, 'Ext_DiasLaborados.py'),
    os.path.join(BASE_PATH, 'Ext_Atencion a clientes.py'),
    os.path.join(BASE_PATH, 'TR_Real.py'),
    os.path.join(BASE_PATH, 'TR_Datos.py'),
]

#  FASE 2.1: MODELADO - DIMENSIONES (DEBE EJECUTARSE ANTES QUE LOS HECHOS)
MODELADO_DIMENSIONES: List[str] = [
    os.path.join(BASE_PATH, 'Dim_Concepto.py'),
    os.path.join(BASE_PATH, 'Dim_Planta.py'),
    os.path.join(BASE_PATH, 'Dim_Empleado.py'),
    os.path.join(BASE_PATH, 'Dim_Planta_clientes.py'),
    os.path.join(BASE_PATH, 'Dim_Cliente.py'),
]

# FASE 2.2: MODELADO - HECHOS (DEBE EJECUTARSE DESPUÉS DE LAS DIMENSIONES)
MODELADO_HECHOS: List[str] = [
    os.path.join(BASE_PATH, 'fctFinanzasDiario.py'),
    os.path.join(BASE_PATH, 'fctAtencionClientes.py'),
]


# Estructura principal de segmentos disponibles
SEGMENTOS_DISPONIBLES: Dict[str, List[str]] = {
    # Flujo Financiero: Extracción y preparación de datos base.
    "FINANCIERO": FINANCIERO_SCRIPTS, 
    # Flujo Modelado: Dimensiones primero, luego Tablas de Hechos.
    "MODELADO": MODELADO_DIMENSIONES + MODELADO_HECHOS, 
}

# -----------------------------------------------------------------------------
# ✅ CONTROL PRINCIPAL: CAMBIE ESTA VARIABLE PARA ELEGIR EL SEGMENTO
# Opciones válidas: "FINANCIERO", "MODELADO", o "TODOS".
# -----------------------------------------------------------------------------
SEGMENTO_A_EJECUTAR: str = "TODOS" 

# Lógica para determinar la lista final de ejecución y el prefijo del log
if SEGMENTO_A_EJECUTAR in SEGMENTOS_DISPONIBLES:
    SCRIPTS_TO_RUN = SEGMENTOS_DISPONIBLES[SEGMENTO_A_EJECUTAR]
    LOG_FILE_PREFIX = f'Proceso_{SEGMENTO_A_EJECUTAR}'
elif SEGMENTO_A_EJECUTAR == "TODOS":
    # 🚨 Flujo completo: Financiero -> Dimensiones -> Hechos
    SCRIPTS_TO_RUN = FINANCIERO_SCRIPTS + MODELADO_DIMENSIONES + MODELADO_HECHOS
    LOG_FILE_PREFIX = 'Proceso_Completo'
else:
    print(f"ERROR: Segmento '{SEGMENTO_A_EJECUTAR}' no reconocido. Ejecutando lista vacía.")
    SCRIPTS_TO_RUN = []
    LOG_FILE_PREFIX = 'ERROR'
    sys.exit(1)


#  3. CONFIGURACIÓN DEL ARCHIVO DE LOG
LOG_BASE_DIR = r'C:\Users\USUARIO\Documents\Juan Manuel Cortes Benitez\Python\Procesamiento_Scripts'


# ==============================================================================
#                      FUNCIÓN DE LOGGING
# ==============================================================================

def log_message(message: str, log_path: str):
    """Añade marca de tiempo al mensaje, lo imprime en consola y lo escribe en el archivo de log."""
    
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    full_message = f"{timestamp} {message}"
    
    # 1. Imprimir en consola
    print(full_message)
    
    # 2. Escribir en archivo de log
    try:
        # Asegurarse de que el directorio del log exista
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(full_message + "\n")
            
    except Exception as e:
        # Esto solo se imprime en consola si falla el log
        print(f"ERROR DE LOGGING: No se pudo escribir en el archivo {log_path}. Razón: {e}")

# ==============================================================================
#                      FUNCIÓN DE EJECUCIÓN
# ==============================================================================

def execute_python_script(script_path: str, python_exe: str, log_path: str) -> bool:
    """Ejecuta un script Python usando subprocess y registra el resultado."""
    
    script_name = os.path.basename(script_path)
    
    # 1. Registrar Inicio
    log_message(f"--- INICIANDO PROCESO: {script_name} ---", log_path)
    
    comando = [python_exe, script_path]
    log_message(f"   Comando: {' '.join(comando)}", log_path)
    
    try:
        # Ejecutar el proceso y capturar la salida
        resultado = subprocess.run(
            comando, 
            capture_output=True, 
            text=True, 
            check=False, # Permite manejar el código de retorno manualmente
            encoding='utf-8',       
            errors='replace',
            env=dict(os.environ, PYTHONIOENCODING='utf-8')      
        )
        
        return_code = resultado.returncode
        
        if return_code == 0:
            # 2. Registrar Éxito
            log_message(f" ÉXITO: {script_name} completado (Código 0).", log_path)
            
            # Registrar la salida estándar (stdout) si existe
            if resultado.stdout and resultado.stdout.strip():
                log_message(f"   Salida Estándar (stdout):\n{resultado.stdout.strip()}", log_path)
            return True
        else:
            # 3. Registrar Fallo
            log_message(f" FALLO: {script_name} finalizó con CÓDIGO DE ERROR {return_code}.", log_path)
            log_message("--- Salida de Error Estándar (stderr) ---", log_path)
            
            # Chequear por None antes de llamar a strip()
            error_output = resultado.stderr if resultado.stderr is not None else ""
            log_message(error_output.strip() or "No hay mensajes de error específicos.", log_path)
            
            log_message("-----------------------------------------", log_path)
            
            if return_code == 2:
                log_message(" DIAGNÓSTICO: Código 2 = Archivo/Comando no encontrado. Revise rutas.", log_path)
            
            return False

    except FileNotFoundError:
        log_message(f" ERROR CRÍTICO: El intérprete '{python_exe}' no fue encontrado.", log_path)
        log_message("   Asegúrese de que Python está en el PATH o que la ruta es correcta.", log_path)
        return False
    except Exception as e:
        log_message(f" ERROR INESPERADO al ejecutar {script_name}: {e}", log_path)
        return False

# ==============================================================================
#                      EJECUCIÓN DEL FLUJO PRINCIPAL
# ==============================================================================

if __name__ == '__main__':
    
    # 0. Generar el nombre de archivo de log dinámico
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_FILE_PATH = os.path.join(LOG_BASE_DIR, f"{LOG_FILE_PREFIX}_{timestamp_str}.log")
    
    # 1. Configuración inicial del log
    log_message(f"\n########################################################", LOG_FILE_PATH)
    log_message(f"####### INICIO DE PROCESAMIENTO SEGMENTADO ###########", LOG_FILE_PATH)
    log_message(f"Log de salida en: {LOG_FILE_PATH}", LOG_FILE_PATH)
    log_message(f"Segmento a ejecutar: {SEGMENTO_A_EJECUTAR} ({len(SCRIPTS_TO_RUN)} scripts)", LOG_FILE_PATH)
    log_message(f"Python Executable: {PYTHON_EXECUTABLE}", LOG_FILE_PATH)
    log_message(f"########################################################\n", LOG_FILE_PATH)
    
    scripts_fallidos = []
    
    # 2. Recorre y ejecuta cada script
    for script_file in SCRIPTS_TO_RUN:
        exito = execute_python_script(script_file, PYTHON_EXECUTABLE, LOG_FILE_PATH)
        
        if not exito:
            scripts_fallidos.append(script_file)
            # break # Descomentar para detener la ejecución inmediatamente después de un fallo.
    
    # 3. Resumen Final
    log_message("\n========================================================", LOG_FILE_PATH)
    log_message("               RESUMEN DE PROCESAMIENTO                 ", LOG_FILE_PATH)
    log_message("========================================================", LOG_FILE_PATH)
    
    if not scripts_fallidos:
        log_message(" Todos los scripts se ejecutaron con éxito.", LOG_FILE_PATH)
        sys.exit(0) # Salida de éxito global
    else:
        log_message(f"🚨 Se detectaron {len(scripts_fallidos)} fallos en los siguientes scripts:", LOG_FILE_PATH)
        for script in scripts_fallidos:
            log_message(f" - {script}", LOG_FILE_PATH)
        
        log_message(f"################ FIN DE PROCESAMIENTO ##################\n", LOG_FILE_PATH)
        sys.exit(1) # Salida de fallo global
