import pyodbc
import configparser
import polars as pl
from pathlib import Path

# Obtener la ruta del archivo database.ini de forma dinámica
CONFIG_PATH = Path(__file__).parent.parent / 'config' / 'database.ini'

def load_db_config():
    """Carga los parámetros de conexión desde el archivo INI."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Error: No se encontró el archivo de configuración en {CONFIG_PATH}")
        
    config = configparser.ConfigParser()
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config.read_file(f)
    
    if 'sql_server_siger' not in config:
        raise ValueError("Error: La sección [sql_server_siger] no se encuentra en database.ini")
        
    return config['sql_server_siger']

def get_db_connection():
    """
    Establece y retorna una conexión a la base de datos SQL Server.
    """
    db_config = load_db_config()
    
    try:
        # Construcción de la cadena de conexión para SQL Server
        conn_str = (
            f"DRIVER={{{db_config.get('driver')}}};"
            f"SERVER={db_config.get('server')};"
            f"DATABASE={db_config.get('database')};"
            f"UID={db_config.get('user')};"
            f"PWD={db_config.get('password')};"
            f"Encrypt={db_config.get('encrypt')};"
            f"TrustServerCertificate={db_config.get('trust_server_certificate')};"
        )
        conn = pyodbc.connect(conn_str)
        print("Conexión a la base de datos SIGER (SQL Server) establecida con éxito. ✅")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error al conectar a la DB (SQLSTATE: {sqlstate}): {ex}")
        print("Asegúrate de tener el DRIVER de ODBC correcto y las credenciales correctas en database.ini.")
        return None

def fetch_data_to_polars(conn: pyodbc.Connection, sql_query: str):
    """
    Ejecuta una consulta SQL y retorna los resultados como un DataFrame de Polars.
    """
    if not conn:
        print("No hay conexión a la base de datos disponible.")
        return pl.DataFrame()
        
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        
        df = pl.DataFrame(data, schema=columns)
        print(f"Datos extraídos: {df.shape[0]} filas, {df.shape[1]} columnas.")
        return df
        
    except pyodbc.Error as ex:
        print(f"Error al ejecutar la consulta: {ex}")
        return pl.DataFrame()
        
    finally:
        if 'cursor' in locals():
            cursor.close()

if __name__ == '__main__':
    # Bloque de prueba para la conexión
    conn = get_db_connection()
    if conn:
        print("Conexión exitosa. Probando a obtener el nombre del servidor...")
        test_query = "SELECT @@SERVERNAME AS ServerName"
        test_df = fetch_data_to_polars(conn, test_query)
        if test_df is not None and not test_df.is_empty():
            print(f"Nombre del Servidor: {test_df.item(0, 'ServerName')}")
        conn.close()
    print("Módulo de conexión finalizado.")