# --- INICIO DEL ARCHIVO src/loader.py ---
import polars as pl
from pathlib import Path
import pyodbc 
from typing import Optional, List, Dict
import configparser 

# ==============================================================================
# CONFIGURACIÓN DE RUTAS
# ==============================================================================
CLEAN_DATA_PATH = Path("../data/clean_data")
# Ruta al archivo de configuración INI (C:\ETL\SIGER_PORTABLE\siger_auditoria_etl\config\database.ini)
CONFIG_FILE = Path(__file__).parent.parent / 'config' / 'database.ini'
SECTION = 'sql_server_siger' # Sección definida por el usuario

# ==============================================================================
# FUNCIONES DE CONEXIÓN Y CARGA
# ==============================================================================

def get_db_connection() -> Optional[pyodbc.Connection]:
    """Establece la conexión a SQL Server leyendo los parámetros del archivo INI."""
    
    config = configparser.ConfigParser()
    if not CONFIG_FILE.exists():
        print(f"  -> ❌ ERROR: Archivo de configuración no encontrado en: {CONFIG_FILE.as_posix()}")
        return None
        
    config.read(CONFIG_FILE.as_posix())
    
    if SECTION not in config:
        print(f"  -> ❌ ERROR: Sección '{SECTION}' no encontrada en el archivo INI.")
        return None

    # Obtener parámetros del INI
    params: Dict[str, str] = dict(config.items(SECTION))
    
    # Construir la Connection String
    conn_str_parts: List[str] = [
        f"DRIVER={params.get('driver')}",
        f"SERVER={params.get('server')}",
        f"DATABASE={params.get('database')}",
        f"UID={params.get('user')}",
        f"PWD={params.get('password')}",
        f"Encrypt={params.get('encrypt', 'yes')}",
        f"TrustServerCertificate={params.get('trust_server_certificate', 'yes')}",
        "Connection Timeout=30"
    ]
    conn_str: str = ";".join(conn_str_parts)

    try:
        conn = pyodbc.connect(conn_str)
        print("  -> Conexión a SQL Server exitosa (vía INI).")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"  -> ❌ ERROR de conexión a SQL Server. SQLSTATE: {sqlstate}")
        return None

def load_to_sql_server(df: pl.DataFrame, table_name: str, conn: pyodbc.Connection):
    """Carga los datos del DataFrame en la tabla de SQL Server (L2) usando pyodbc."""
    
    cursor = conn.cursor()
    print(f"  -> Preparando inserción masiva en la tabla '{table_name}'...")
    
    try:
        placeholders: str = ', '.join(['?' for _ in df.columns])
        sql_insert: str = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
        
        data: List[tuple] = [tuple(row.values()) for row in df.to_dicts()]
        cursor.executemany(sql_insert, data)
        conn.commit()
        
        print(f"  -> ✅ Carga L2 a SQL Server exitosa: {len(data)} filas insertadas en {table_name}.")

    except Exception as e:
        conn.rollback()
        print(f"  -> ❌ FALLO en la inserción masiva a {table_name}. ERROR SQL Server/ODBC: {str(e)}")
    finally:
        cursor.close()

# ==============================================================================
# FUNCIONES DE CARGA
# ==============================================================================

def load_to_parquet(df: pl.DataFrame, table_name: str, output_path: Path):
    """Carga el DataFrame limpio en un archivo Parquet (L1)."""
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / f"{table_name}.parquet"
    print(f"  -> Guardando {table_name} en Parquet: {file_path.as_posix()}")
    
    try:
        df.write_parquet(file=file_path.as_posix(), compression="zstd")
        print(f"  -> ✅ Carga L1 exitosa: {df.shape[0]} filas cargadas en Parquet.")
    except Exception as e:
        print(f"  -> ❌ ERROR durante la carga L1 a Parquet: {str(e)}")


def apply_loading(table_name: str, df: pl.DataFrame) -> None:
    """Función principal que dirige el proceso de carga L1 (Parquet) y L2 (SQL Server)."""
    print(f"--- INICIANDO CARGA (L) para {table_name} ---")
    
    # L1: Cargar a Parquet (Staging local)
    load_to_parquet(df, table_name, CLEAN_DATA_PATH)
    
    # L2: Cargar a SQL Server
    conn = get_db_connection()
    if conn:
        load_to_sql_server(df, table_name, conn)
        conn.close()
    
    print("--- CARGA (L) FINALIZADA ---")

# --- FIN DEL ARCHIVO src/loader.py ---