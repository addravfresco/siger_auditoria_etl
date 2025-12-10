import polars as pl
import os
import sys
from pathlib import Path
from datetime import datetime

# === Configuración de Paths ===
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, os.path.dirname(project_root))

# Importamos el extractor y las funciones de carga de configuración
from src.extractor import extract_from_file
from src.analyzer import load_paths_config_root 


def log_anomalies(df_anomalies: pl.DataFrame, table_name: str):
    """Exporta el DataFrame de anomalías a un archivo CSV."""
    log_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent / 'data' / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = log_dir / f"{table_name}_anomalies_{timestamp}.csv"
    
    # Exportar el DataFrame de anomalías
    df_anomalies.write_csv(file_path, separator='|')
    
    print(f"\n {df_anomalies.shape[0]} Anomalías registradas en: {file_path}")


def transform_mvsolicitudes(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Identifica anomalías, las registra, limpia las fechas y elimina columnas nulas.
    
    :param df: Polars DataFrame de MVSOLICITUDES.
    :return: Una tupla con (DataFrame Limpio, DataFrame de Anomalías).
    """
    
    #1. Conversión y Limpieza de Fechas
    date_cols = ['FCINGRESO', 'FCLIMITE', 'FCLIMITESUBS']
    
    df_temp = df.clone()
    
    # 1. Convertir todas las columnas de fecha a Datetime
    for col in date_cols:
        df_temp = df_temp.with_columns(
            pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False).alias(f'{col}_DT')
        )
        
    # 2. Trazabilidad y Filtro Maestro
    
    fecha_anomalias = pl.Series([False] * df.shape[0], dtype=pl.Boolean) 
    
    for col in date_cols:
        col_dt = f'{col}_DT'
        fecha_anomalias = fecha_anomalias | ( (pl.col(col_dt).dt.year() == 4) | (pl.col(col_dt).dt.year() == 9998) )

    # Filtro maestro: una fila es anómala si hay anomalía de fecha O DSNCI es 'BORRADO'
    borrado_filter = pl.col('DSNCI') == 'BORRADO'
    anomalies_filter = fecha_anomalias | borrado_filter

    # DataFrame de Anomalías: filas que cumplen el filtro maestro
    df_anomalies = df_temp.filter(anomalies_filter)
    
    # Agregar la columna de tipo de anomalía para el log
    df_anomalies = df_anomalies.with_columns(
        pl.when(pl.col('DSNCI') == 'BORRADO')
          .then(pl.lit("DSNCI_BORRADO"))
          .otherwise(pl.lit("FECHA_EXTREMA"))
          .alias("Tipo_Anomalia")
    ).select(df.columns + ['Tipo_Anomalia']) 
    
    # --- 3. Aplicar Limpieza al DataFrame Principal ---
    
    # Reemplazar las anomalías temporales por Null en las nuevas columnas *_DT
    df_cleaned = df_temp.with_columns([
        pl.when((pl.col(f'{col}_DT').dt.year() == 4) | (pl.col(f'{col}_DT').dt.year() == 9998))
          .then(None) 
          .otherwise(pl.col(f'{col}_DT'))
          .alias(f'{col}_DT')
        for col in date_cols
    ])
    
    # Limpieza de Columnas (Basado en EDA: 100% o casi 100% de nulos)
    cols_to_drop = [
        'LLSOLICITUD_2', 
        'LLPAGOSSRPETUSU', 
        'LLPAGOSSINREG', 
        # Eliminar las columnas originales de string de fecha
    ] + date_cols
    
    df_clean_final = df_cleaned.drop(cols_to_drop)
    
    print(f"\n Transformaciones aplicadas. Filas: {df_clean_final.shape[0]}, Columnas: {df_clean_final.shape[1]}")
    
    return df_clean_final, df_anomalies


def main():
    print("--- INICIANDO FASE DE TRANSFORMACIÓN (T) PARA MVSOLICITUDES ---")
    
    try:
        # 1. Extracción (E)
        siger_files_root = load_paths_config_root()
        solicitudes_df = extract_from_file("MVSOLICITUDES", root_path=siger_files_root, limit=None)
        
        # 2. Transformación (T)
        df_clean, df_anomalies = transform_mvsolicitudes(solicitudes_df)
        
        # 3. Carga de Log (L)
        if not df_anomalies.is_empty():
            log_anomalies(df_anomalies, "MVSOLICITUDES")
        else:
            print("\n No se encontraron anomalías de fecha/DSNCI 'BORRADO'. No se generó log.")
            
        # 4. Mostrar DataFrame limpio
        print("\n--- Muestra del DataFrame LIMPIO (MVSOLICITUDES) ---")
        print(df_clean.head(5))
        print("\nEsquema final (Tipos de datos):")
        print(df_clean.schema)
        
    except Exception as e:
        print(f"\n Fallo en la Transformación. Mensaje: {e}")

if __name__ == '__main__':
    main()