import logging
import zipfile
import os
import pandas as pd
from typing import List, Optional

logger = logging.getLogger(__name__)

class ZipProcessor:
    """Processes ZIP files containing CSV data using in-memory extraction."""

    def inspect_zip(self, zip_path: str) -> List[str]:
        """
        Opens a ZIP file (read-only) and returns a list of filenames inside it.
        Does NOT extract the files yet.
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                file_list = z.namelist()
                logger.info(f"Inspecting {os.path.basename(zip_path)}: Found {len(file_list)} files.")
                return file_list
        except zipfile.BadZipFile:
            logger.error(f"Error: {zip_path} is corrupted or not a valid ZIP.")
            return []
        except Exception as e:
            logger.error(f"Error reading {zip_path}: {e}")
            return []

    def read_csv_from_zip(self, zip_path: str, target_filename: str) -> Optional[pd.DataFrame]:
        """
        Extracts a specific file from the ZIP and reads it into a Pandas DataFrame.
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                # open the file directly from memory (no need to extract to disk first)
                with z.open(target_filename) as f:
                    logger.info(f"Reading {target_filename}...")
                    
                    df = pd.read_csv(f, encoding='utf-8', sep=';', dtype=str)



                    if 'DESCRICAO' in df.columns:
                        df['DESCRICAO'] = df['DESCRICAO'].str.strip()

                        target_pattern = "Despesas com Eventos / Sinistros"

                        mask = df['DESCRICAO'].str.contains(target_pattern, case=False, na=False, regex=False)

                        df_filtered = df[mask].copy()


                    numeric_cols = ['VL_SALDO_INICIAL', 'VL_SALDO_FINAL']
                    for col in numeric_cols:
                        if col in df_filtered.columns:
                            df_filtered[col] = df_filtered[col].apply(self._to_float)
                    
                    logger.info(f"Filtered: {len(df_filtered)} rows match '{target_pattern}'.")
                    return df_filtered

        except Exception as e:
            logger.error(f"Error reading {target_filename}: {e}")
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
            logger.warning(f"No suitable CSV/XLSX found in {zip_path}")
            return None
        
        return self.read_csv_from_zip(zip_path, target_file)
