# --- INICIO DEL ARCHIVO src/transformer.py ---
import polars as pl
from typing import Dict, Any, List

# Columnas identificadas con 100% de nulos o irrelevantes, listas para ser descartadas.
COLUMNS_TO_DROP: Dict[str, List[str]] = {
    'CFVARIABLES': ['CFPAGODERECHOS_LLOFICINA'],
    'CTOFICINAS': ['LLPROCESO'],               
}

# Transformaciones de Tipo de Dato o Limpieza Específica
def transform_ctoficinas(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformaciones a la tabla CTOFICINAS."""
    print("  -> Limpiando CTOFICINAS...")
    
    df = df.drop(COLUMNS_TO_DROP.get('CTOFICINAS', []))

    df = df.with_columns(
        [
            pl.col(c).cast(pl.Utf8).fill_null("").alias(c)
            for c in ['DSEXTENCION', 'DSNOMBRERESP', 'DSPAGINAWEB'] 
            if c in df.columns
        ]
    )
    
    df = df.with_columns(
        [
            pl.col(c).str.to_date(format="%d/%m/%Y %H:%M:%S", strict=False).alias(c)
            for c in ['FCINIOPERACION', 'FCALTA']
            if c in df.columns
        ]
    )
    
    if 'LLOFICINAMG' in df.columns:
        df = df.with_columns(
            pl.col("LLOFICINAMG").fill_null(-1).cast(pl.Int64).alias("LLOFICINAMG")
        )

    print(f"  -> CTOFICINAS: Filas {df.shape[0]}, Columnas {df.shape[1]}")
    return df


def transform_cfvariables(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformaciones a la tabla CFVARIABLES."""
    print("  -> Limpiando CFVARIABLES...")
    
    # 1. Descarte de columna 100% nula
    df = df.drop(COLUMNS_TO_DROP.get('CFVARIABLES', []))
    
    print(f"  -> CFVARIABLES: Filas {df.shape[0]}, Columnas {df.shape[1]}")
    return df


def transform_ctsocios(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformaciones a la tabla CTSOCIOS."""
    print("  -> Limpiando CTSOCIOS...")
    
    df = df.with_columns(
        pl.col("FCCONSTITUCION").str.to_date(format="%d/%m/%Y %H:%M:%S", strict=False).alias("FCCONSTITUCION"),
        pl.col("FCNACIMIENTO").str.to_date(format="%d/%m/%Y %H:%M:%S", strict=False).alias("FCNACIMIENTO") # <-- NUEVA CONVERSIÓN
    )

    df = df.with_columns(
        [
            pl.col(c)
            .cast(pl.Utf8, strict=False)
            .fill_null("")  
            .alias(c)
            for c in ["DSCURP", "DSRFC", "DSDOMICILIO"] 
        ]
    )
    
    print(f"  -> CTSOCIOS: Filas {df.shape[0]}, Columnas {df.shape[1]}")
    return df



def transform_default(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """Transformación por defecto: solo garantiza que los tipos de texto sean Utf8."""
    
    
    
    df = df.with_columns(
        [
            pl.col(c).cast(pl.Utf8) 
            for c in df.columns 
            if df[c].dtype == pl.String 
        ]
    )
    return df

# ==============================================================================
# MAPEO DE FUNCIONES
# ==============================================================================

# Mapeo de la tabla a la función de transformación
TRANSFORM_FUNCTIONS: Dict[str, Any] = {
    'CTOFICINAS': transform_ctoficinas,
    'CFVARIABLES': transform_cfvariables,
    'CTSOCIOS': transform_ctsocios,
}

def apply_transformation(table_name: str, df: pl.DataFrame) -> pl.DataFrame:
    """Dirige la transformación al motor de limpieza específico o al motor por defecto."""
    transform_func = TRANSFORM_FUNCTIONS.get(table_name, transform_default)
    
    if transform_func == transform_default:
        return transform_default(df, table_name)
    else:
        return transform_func(df)

