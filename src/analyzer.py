import polars as pl
import configparser
from pathlib import Path
import os
import sys

# === CORRECCIÓN FINAL DE AMBIENTE ===
# 1. Agrega el directorio raíz del proyecto ('siger_auditoria_etl') al sys.path
# Esto resuelve los problemas de Importación y el "Error Crítico: No se pudo importar la configuración".
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    # Inserta la carpeta padre (siger_auditoria_etl) en sys.path
    sys.path.insert(0, os.path.dirname(project_root))
# ------------------------------------

# La importación ahora es absoluta, confiando en la corrección de sys.path
from src.extractor import extract_from_file

# Configuración de Paths: Calculamos la ruta relativa al archivo paths.ini
CONFIG_PATH = Path(__file__).parent.parent / 'config' / 'paths.ini'

def load_paths_config_root() -> Path:
    """Carga la ruta raíz de los archivos fuente desde paths.ini."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Error: No se encontró el archivo de configuración en {CONFIG_PATH}")
        
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    
    if 'source_paths' not in config:
        raise ValueError("Error: La sección [source_paths] no se encuentra en paths.ini")
        
    return Path(config['source_paths'].get('siger_files_root'))


def run_eda_solicitudes(df: pl.DataFrame):
    """Realiza un Análisis Exploratorio de Datos (EDA) en MVSOLICITUDES."""
    
    print("\n--- INICIANDO ANÁLISIS DE CALIDAD DE DATOS (EDA) ---")
    
    pk_col = 'LLSOLICITUD'
    total_rows = df.shape[0]

    # [1] Validación de Clave Primaria (LLSOLICITUD)
    print("\n[1] Validación de Clave Primaria (LLSOLICITUD):")
    unique_count = df[pk_col].n_unique()
    null_count = df[pk_col].null_count()
    
    print(f"Total de Filas: {total_rows}")
    print(f"Valores Únicos ({pk_col}): {unique_count}")
    print(f"Valores Nulos ({pk_col}): {null_count}")

    if unique_count == total_rows and null_count == 0:
        print("✅ LLSOLICITUD es una Clave Primaria (PK) válida (100% único, 0 nulos).")
    else:
        print(f"❌ LLSOLICITUD no es PK perfecta. Duplicados: {total_rows - unique_count}. Nulos: {null_count}.")


    # [2] Conteo de Valores Nulos (Calidad de Datos) - CORRECCIÓN FINAL
    print("\n[2] Conteo de Valores Nulos (Calidad de Datos):")
    
    # 1. Crear una proyección donde cada columna se convierte en su conteo de nulos
    null_counts_exprs = [pl.col(c).null_count().alias(c) for c in df.columns]
    
    # 2. Ejecutar la proyección y transponerla SIN incluir encabezado
    # La columna 0 contendrá los conteos.
    null_analysis = df.select(null_counts_exprs).transpose(include_header=False)
    
    # 3. Asignar nombres: Columna 0 es 'Nulos', Columna 1 en adelante son los nombres de las columnas originales
    null_analysis = null_analysis.rename({null_analysis.columns[0]: 'Nulos'})

    # 4. Asignar los nombres de las columnas a una nueva Serie 'Columna'
    null_analysis = null_analysis.with_columns(pl.Series("Columna", df.columns))
    
    # 5. La columna 'Nulos' ya es el resultado de pl.null_count(), que es un Int.
    # Ahora podemos calcular el porcentaje sin el error de conversión.
    null_analysis = null_analysis.with_columns([
        (pl.col('Nulos') / total_rows * 100).round(2).alias('Porcentaje_Nulos')
    ])
    
    # 6. Seleccionar y ordenar
    null_analysis = null_analysis.select(['Columna', 'Nulos', 'Porcentaje_Nulos']).sort(pl.col('Nulos'), descending=True)

    print(null_analysis)


    # [3] Conversión y Análisis de Claves Temporales (Fechas)
    print("\n[3] Conversión y Análisis de Claves Temporales (Fechas):")
    date_cols = ['FCINGRESO', 'FCLIMITE', 'FCLIMITESUBS']
    
    df_dates = df.clone()

    try:
        for col in date_cols:
             if col in df.columns:
                df_dates = df_dates.with_columns(
                    pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S%.f").alias(f'{col}_DT')
                )
                
                min_date = df_dates[f'{col}_DT'].min()
                max_date = df_dates[f'{col}_DT'].max()
                
                print(f"  Análisis {col}:")
                print(f"    Rango: {min_date} a {max_date}")
                
                # Búsqueda de Anomalías Temporales (Fechas Futuras/Fechas Vacías)
                anom_count = df_dates.filter(pl.col(f'{col}_DT') > pl.lit("2026-01-01")).shape[0]
                print(f"    Anomalías (Fechas Futuras > 2026-01-01): {anom_count} filas.")

        print("✅ Conversión y análisis de fechas completado.")

    except Exception as e:
        print(f"❌ Fallo al convertir una columna de fecha. Error: {e}")
        
    # [4] Análisis de Columna Categórica (LLOFICINA - Clave Foránea de Oficina)
    print("\n[4] Análisis de Columna Categórica (LLOFICINA):")
    
    oficina_counts = df.group_by("LLOFICINA").len().sort("len", descending=True).limit(10)
    print("Top 10 LLOFICINA por Frecuencia:")
    print(oficina_counts)
    
    oficina_nulos = df.filter(pl.col("LLOFICINA").is_null()).shape[0]
    print(f"LLOFICINA Nulos: {oficina_nulos}")


def main():
    print("--- INICIANDO CARGA COMPLETA DE MVSOLICITUDES PARA EDA ---")
    
    try:
        # 1. Cargamos la ruta raíz de los archivos SIGER
        siger_files_root = load_paths_config_root()
        
        # 2. Cargar TODAS las filas de MVSOLICITUDES (limit=None)
        solicitudes_df = extract_from_file("MVSOLICITUDES", root_path=siger_files_root, limit=None)
    
        if solicitudes_df is not None:
            run_eda_solicitudes(solicitudes_df)
            
    except Exception as e:
        print(f"\n❌ Fallo en la carga completa: {e}")
        
if __name__ == '__main__':
    main()