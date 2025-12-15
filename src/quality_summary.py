# --- INICIO DEL ARCHIVO src/quality_summary.py ---
import polars as pl
from pathlib import Path
import re
from datetime import datetime 

def summarize_eda_reports(reports_path: Path):
    """
    Carga todos los reportes EDA (Calidad de Datos) generados y los consolida 
    en un único DataFrame para una revisión de alto nivel.
    """
    print("--- INICIANDO CONSOLIDACIÓN DE REPORTES EDA ---")
    
    # Busca todos los archivos CSV que terminan en 'EDA_Report_*.csv'
    report_files = list(reports_path.glob('*_EDA_Report*.csv'))
    
    if not report_files:
        print(f"❌ No se encontraron archivos de reporte EDA en {reports_path.as_posix()}")
        return None

    all_summaries = []
    
    for file_path in report_files:
        try:
            # Extraer el nombre de la tabla del nombre del archivo
            table_name_match = re.search(r'([A-Z_]+)_EDA_Report_', file_path.name)
            table_name = table_name_match.group(1) if table_name_match else "UNKNOWN"
            
            # Cargar el reporte
            df_report = pl.read_csv(file_path, separator='|', encoding='utf-8')
            
            
            df_summary = df_report.select(
                pl.lit(table_name).alias("Tabla"),
                pl.col("Columna"),
                pl.col("Tipo_Dato").alias("Tipo_Original"), # <--- CORREGIDO: Usamos Tipo_Dato y lo renombramos a Tipo_Original
                pl.col("Nulos").alias("Total_Nulos"),
                (pl.col("Porcentaje_Nulos") / 100).round(2).alias("Porcentaje_Nulos_Pct") 
            )
            all_summaries.append(df_summary)
            print(f"✅ Cargado reporte para: {table_name}")

        except Exception as e:
            print(f"❌ Error al cargar {file_path.name}: {e}")

    if not all_summaries:
        return None

    # Concatenar todos los DataFrames en un resumen maestro
    df_master = pl.concat(all_summaries, how="diagonal_relaxed").sort(["Tabla", "Porcentaje_Nulos_Pct"], descending=[False, True])

    # Guardar el resumen maestro
    output_path = reports_path / f"MASTER_QUALITY_SUMMARY_{datetime.now():%Y%m%d_%H%M%S}.csv"
    df_master.write_csv(output_path.as_posix())
    
    print("\n--- RESUMEN MAESTRO GENERADO ---")
    print(f"Total de {df_master.shape[0]} métricas de calidad de datos guardadas en:")
    print(f"🔗 {output_path.name}")
    print("\nMostrando las 10 columnas con más valores nulos (Excluyendo Nulos=0):")
    
    # Mostrar top 10 columnas con más nulos
    top_nulls = df_master.filter(pl.col("Total_Nulos") > 0).head(10)
    print(top_nulls)
    
    return output_path

if __name__ == "__main__":
    # La ruta de los reportes es la misma carpeta donde el analyzer guarda los CSV de EDA
    REPORTS_DIR = Path("data/reports")
    summarize_eda_reports(REPORTS_DIR)

# --- FIN DEL ARCHIVO src/quality_summary.py ---