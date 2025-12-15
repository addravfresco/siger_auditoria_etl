# --- INICIO DEL ARCHIVO src/analyzer.py ---
import sys
import os
from pathlib import Path
import polars as pl
from typing import Dict, List
import time
import datetime

current_dir = Path(__file__).resolve().parent
sys.path.append(current_dir.as_posix())

# Importar funciones de los módulos E, T y L
from extractor import extract_from_file
from transformer import apply_transformation
from loader import apply_loading 


# RUTA ABSOLUTA DE LOS DATOS FUENTE (Unidad de red Z:)
ROOT_DATA_PATH = Path(r"Z:\Fuentes Originales\Siger\SIGER_2025\SIGER\entrega_siger.tar\entrega_siger\respaldos\Entrega\tables_siger")

# Directorios de salida
BASE_DIR = current_dir.parent
REPORTS_DIR = BASE_DIR / 'data' / 'reports'
ANOMALIES_DIR = BASE_DIR / 'anomalies'

# Asegurar que los directorios existan
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
ANOMALIES_DIR.mkdir(parents=True, exist_ok=True)
(BASE_DIR / 'data' / 'clean_data').mkdir(parents=True, exist_ok=True) # Asegurar carpeta de Parquet

# Lista de tablas a procesar
TABLES_TO_PROCESS: List[str] = [
    'MVCARATULAS'
    # 'MVSOLICITUDES', 'MVFRMACTO', 'CTSOCIOS',
    # 'MVVARACTO', 'MVDOCADJUNTOS', 'CFVARIABLES', 'CTUSUARIOS',
    # 'PAGO_PORTAL', 'CTGIROS', 'CTOFICINAS', 'CTTIPOSOCIEDAD'
]

# LÓGICA DE ANÁLISIS DE CALIDAD DE DATOS (EDA)


def analyze_data_quality(df: pl.DataFrame, table_name: str, reports_dir: Path):
    """
    Realiza un análisis básico de calidad de datos (nulos, tipos)
    y genera un reporte CSV.
    """
    print(f"\n--- INICIANDO ANÁLISIS DE CALIDAD DE DATOS para {table_name} ---")
    
    # 1. Conteo de Valores Nulos
    print("[1] Conteo de Valores Nulos (Calidad de Datos):")
    
    # Calcular métricas de calidad
    null_counts = []
    total_rows = df.shape[0]
    
    for col in df.columns:
        # Contar nulos
        null_count = df[col].is_null().sum()
        
        # Obtener el tipo de dato
        dtype = str(df[col].dtype)
        
        # Calcular porcentaje
        if total_rows > 0:
            null_percentage = (null_count / total_rows)
        else:
            null_percentage = 0.0
            
        null_counts.append({
            'Tabla': table_name,
            'Columna': col,
            'Tipo_Original': dtype,
            'Total_Filas': total_rows,
            'Total_Nulos': null_count,
            'Porcentaje_Nulos_Pct': null_percentage
        })

    # Crear DataFrame de reporte
    report_df = pl.DataFrame(null_counts)
    
    # Generar timestamp para el nombre del archivo
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"{table_name}_EDA_Report_{timestamp}.csv"
    report_path = reports_dir / report_filename
    
    # Guardar el reporte
    report_df.write_csv(report_path.as_posix())
    
    print(f"Reporte generado. Filas: {total_rows}")
    print(f"✅ Reporte EDA generado: {report_path.name}")
    print(f"| ✅ EDA finalizado para {table_name}.")

# ==============================================================================
# FUNCIÓN PRINCIPAL DEL PIPELINE
# ==============================================================================

def main():
    """Ejecuta el pipeline E-T-L-EDA para todas las tablas."""
    print("--- INICIANDO PIPELINE ETL Y QA ---")
    
    for table_name in TABLES_TO_PROCESS:
        
        print("\n" + "=" * 55)
        print(f"| 📊 Procesando Tabla: {table_name}")
        print("=" * 55)
        
        try:
            # 1. Extracción (E)
            df = extract_from_file(table_name, ROOT_DATA_PATH, ANOMALIES_DIR)
            
            # 2. Transformación (T)
            print(f"--- INICIANDO TRANSFORMACIÓN para {table_name} ---")
            df = apply_transformation(table_name, df)
            print("--- TRANSFORMACIÓN FINALIZADA ---")

            # 3. Carga (L) - L1 (Parquet) y L2 (SQL Server)
            apply_loading(table_name, df) 

            # 4. Análisis Exploratorio de Datos (EDA)
            analyze_data_quality(df, table_name, REPORTS_DIR)

        except Exception as e:
            print(f"| ❌ FALLO CRÍTICO en el Pipeline para {table_name}. Mensaje:")
            print(f"'{e}'")
            print("=" * 55)

    print("\n--- PIPELINE COMPLETO FINALIZADO ---")

if __name__ == '__main__':
    main()
# --- FIN DEL ARCHIVO src/analyzer.py ---