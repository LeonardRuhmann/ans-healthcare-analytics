import zipfile
import os
import pandas as pd
from typing import List, Optional

class ZipProcessor:
    def __init__(self, temp_dir="temp_extract"):
        """
        Initializes the processor.
        temp_dir: Where we will extract files if necessary.
        """
        self.temp_dir = temp_dir
        os.makedirs(self.temp_dir, exist_ok=True)

    def inspect_zip(self, zip_path: str) -> List[str]:
        """
        Opens a ZIP file (read-only) and returns a list of filenames inside it.
        Does NOT extract the files yet.
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                file_list = z.namelist()
                print(f"Inspecting {os.path.basename(zip_path)}: Found {len(file_list)} files.")
                return file_list
        except zipfile.BadZipFile:
            print(f"Error: {zip_path} is corrupted or not a valid ZIP.")
            return []
        except Exception as e:
            print(f"Error reading {zip_path}: {e}")
            return []

    def read_csv_from_zip(self, zip_path: str, target_filename: str) -> Optional[pd.DataFrame]:
        """
        Extracts a specific file from the ZIP and reads it into a Pandas DataFrame.
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                # open the file directly from memory (no need to extract to disk first)
                with z.open(target_filename) as f:
                    print(f"Reading {target_filename}...")
                    
                    df = pd.read_csv(f, encoding='utf-8', sep=';', dtype=str)

                    numeric_cols = ['VL_SALDO_INICIAL', 'VL_SALDO_FINAL']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].apply(self._to_float)
                    print(f"Loaded {len(df)} rows. Transforming...")

                    target_pattern = r"Despesas com Eventos\s*/\s*Sinistros"

                    mask = df['DESCRICAO'].str.contains(target_pattern, case=False, na=False, regex=True)

                    df_filtered = df[mask].copy()
                    
                    print(f"Filtered: {len(df_filtered)} rows match regex '{target_pattern}'.")
                    return df_filtered

        except Exception as e:
            print(f"Error reading {target_filename}: {e}")
            return None

    def _to_float(self, val):
        """
        Helper: Converte string '1.000,00' para float 1000.00
        """
        if isinstance(val, str):
            # Remove thousands separator and replace decimal comma with point
            # E.g.: '1.234,56' -> '1234.56'
            clean_val = val.replace('.', '').replace(',', '.')
            try:
                return float(clean_val)
            except ValueError:
                return 0.0
        return val

    def process_zip(self, zip_path: str) -> Optional[pd.DataFrame]:
        """
        Orchestrator wrapper:
        1. Inspects the ZIP.
        2. Finds the likely data file (CSV/XLSX).
        3. Reads and filters it.
        """
        files = self.inspect_zip(zip_path)
        
        # Heuristic: Find the first file that ends with .csv or .xlsx and isn't a readme
        target_file = next((f for f in files if f.lower().endswith(('.csv', '.xlsx'))), None)
        
        if not target_file:
            print(f"No suitable CSV/XLSX found in {zip_path}")
            return None
        
        return self.read_csv_from_zip(zip_path, target_file)