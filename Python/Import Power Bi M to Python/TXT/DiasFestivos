import holidays
from datetime import date
import pandas as pd

# Diccionario de traducción robusto: Usaremos MAYÚSCULAS para asegurar la coincidencia.
TRADUCCION_FESTIVOS_MX_ROBUSTO = {
    "NEW YEAR'S DAY": "Año Nuevo",
    "CONSTITUTION DAY": "Día de la Constitución",
    "BENITO JUÁREZ'S BIRTHDAY": "Natalicio de Benito Juárez",
    "BENITO JUAREZ'S BIRTHDAY": "Natalicio de Benito Juárez", # Variación sin acento
    "LABOR DAY": "Día del Trabajo", # Variación de inglés americano (el que aparece en tu salida)
    "LABOUR DAY": "Día del Trabajo", # Variación de inglés británico
    "INDEPENDENCE DAY": "Día de la Independencia",
    "REVOLUTION DAY": "Día de la Revolución",
    "CHANGE OF FEDERAL EXECUTIVE POWER": "Transmisión del Poder Ejecutivo Federal",
    "CHRISTMAS DAY": "Navidad",
    # Incluir otros días comunes (Aunque no sean feriados obligatorios, se traducen si aparecen)
    "MOTHER'S DAY": "Día de la Madre",
    "ALL SOULS' DAY": "Día de Muertos",
    "DAY OF THE DEAD": "Día de Muertos",
    "OUR LADY OF GUADALUPE DAY": "Día de la Virgen de Guadalupe",
    "MAUNDY THURSDAY": "Jueves Santo",
    "GOOD FRIDAY": "Viernes Santo",
}


def generar_dias_festivos_mexico(año_inicio: int, año_fin: int) -> pd.DataFrame:
    """
    Genera una lista de días festivos oficiales de México para un rango de años,
    con los nombres traducidos al español de forma robusta.
    """
    
    # 1. Generar los días festivos
    mex_holidays = holidays.MX(years=range(año_inicio, año_fin + 1))
    
    festivos_data = [
        {'Fecha': fecha, 'Festivo': nombre} 
        for fecha, nombre in mex_holidays.items()
    ]
    
    df_festivos = pd.DataFrame(festivos_data)
    
    # 2. TRADUCCIÓN ROBUSTA A ESPAÑOL
    # A. Crear columna temporal en mayúsculas para un mapeo sin errores de case
    df_festivos['FESTIVO_UPPER'] = df_festivos['Festivo'].astype(str).str.strip().str.upper()
    
    # B. Mapear la traducción
    df_festivos['Festivo_Traducido'] = df_festivos['FESTIVO_UPPER'].map(TRADUCCION_FESTIVOS_MX_ROBUSTO)
    
    # C. Usar la traducción si existe, sino usar el nombre original (que podría estar ya en español o ser poco común)
    df_festivos['Festivo'] = df_festivos['Festivo_Traducido'].fillna(df_festivos['Festivo'])
    
    # D. Limpieza y tipado final
    df_festivos = df_festivos.drop(columns=['FESTIVO_UPPER', 'Festivo_Traducido'], errors='ignore')
    df_festivos['Fecha'] = pd.to_datetime(df_festivos['Fecha'])
    df_festivos['Festivo'] = df_festivos['Festivo'].astype(str).str.strip()
    
    df_festivos = df_festivos.sort_values(by='Fecha').reset_index(drop=True)
    
    return df_festivos

# --- Ejemplo de uso ---
# El output del usuario muestra 2025 y 2026, así que lo forzaremos a esos años.
df_dias_festivos = generar_dias_festivos_mexico(2025, 2026)

print(f"✅ Días Festivos Oficiales de México (2025-2026) - TRADUCIDOS")
print("-" * 50)
print(df_dias_festivos.head(15).to_markdown(index=False))
print("\nTipos de datos de las columnas:")
print(df_dias_festivos.dtypes)