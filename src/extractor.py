import polars as pl
import configparser
from pathlib import Path
import os
import sys

# La función de extracción ahora recibe la ruta raíz directamente
def extract_from_file(table_name: str, root_path: Path, limit: int = None) -> pl.DataFrame:
    """
    Extrae datos de una tabla usando Polars.
    
    :param table_name: Nombre de la tabla (ej. MVSOLICITUDES).
    :param root_path: Ruta del directorio donde se encuentran los archivos fuente.
    :param limit: Número máximo de filas a cargar (None para cargar todas).
    :return: Polars DataFrame.
    """
    
    file_path_csv = root_path / f"{table_name}.csv"
    
    if not file_path_csv.exists():
        raise FileNotFoundError(f"Error: No se encontró el archivo para '{table_name}' (.csv) en la ruta: {root_path}")
    
    # --- CORRECCIÓN DE PARSING
    schema_overrides = {
        "DSNCI": pl.Utf8, 
    }
    # ----------------------------------------------------------------------

    print(f"Extrayendo datos de: {file_path_csv}")
    
    df = pl.read_csv(
        file_path_csv, 
        separator='|', 
        infer_schema_length=10000,
        schema_overrides=schema_overrides, 
        n_rows=limit,
        encoding="latin1"
    )
    
    print(f"Datos extraídos: {df.shape[0]} filas, {df.shape[1]} columnas.")
    return df
