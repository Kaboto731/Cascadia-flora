"""Load thinned presences from the DuckDB gold layer."""

import duckdb
import pandas as pd


def load_presences(db_path, table="int_occurrences__thinned") -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            f"select species_key, species_name, lon, lat, worldclim_res from {table}"
        ).df()
    finally:
        con.close()
    return df
