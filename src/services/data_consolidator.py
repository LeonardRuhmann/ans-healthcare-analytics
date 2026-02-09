import pandas as pd
import os
import zipfile
import logging
from src import config

logger = logging.getLogger(__name__)

class DataConsolidator:
    def __init__(self, output_dir=config.OUTPUT_DIR):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def consolidate(self, df: pd.DataFrame):
        """
        Executes the consolidation stage of the ETL pipeline.

        Responsibilities:
        1. Temporal Normalization: Standardizes date formats to Year/Quarter.
        2. Data Cleaning: Filters out null accounting entries (zero values).
        3. Deduplication: Identifies and removes redundant records based on composite keys.
        4. Export: Generates the final dataset in CSV format compressed as ZIP.

        Args:
            df (pd.DataFrame): The raw aggregated DataFrame containing financial data from all quarters.
        """

        initial_count = len(df)

        logger.info(f"--- STARTING CONSOLIDATION (Input: {initial_count} rows) ---")
        

        # STANDARDIZE DATE 
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
        df['Ano'] = df['DATA'].dt.year
        df['Trimestre'] = df['DATA'].dt.quarter

        # RENAME COLUMNS
        df.rename(columns={'VL_SALDO_FINAL': 'ValorDespesas'}, inplace=True)
        
        # Filter Zeros, Keep Negatives
        zeros_mask = (df['ValorDespesas'] == 0)
        zeros_count = zeros_mask.sum()
        
        
        # APPLY FILTER: Keep only non-zeros
        df = df[~zeros_mask].copy()
        
        logger.info(f"    Dropped {zeros_count} rows with Zero Value.")
        logger.info(f"    Keeping {len(df[df['ValorDespesas'] < 0])} negative rows (reversals).")

        # Drop Duplicates
        target_cols = ['REG_ANS', 'DATA', 'CD_CONTA_CONTABIL', 'ValorDespesas', 'DESCRICAO']
        duplicates_mask = df.duplicated(subset=target_cols, keep=False)
        duplicate_count = duplicates_mask.sum()

        if duplicate_count > 0:
            logger.info(f"    Detected {duplicate_count} duplicate records.")
            logger.info("   --- Sample of Duplicates (First 5) ---")
            logger.info("\n" + df[duplicates_mask].head(5).to_string(index=False)) # Cleaner print
            
            #  Remove duplicates, keeping the first occurrence
            df_clean = df.drop_duplicates(subset=target_cols)
            logger.info(f"    Deduplication complete. Dropped {initial_count - len(df_clean)} redundant rows.")
        else:
            logger.info("    No duplicates detected.")
            df_clean = df

        # Create placeholders for CNPJ/Name 
        df_clean = df_clean.assign(CNPJ=None, RazaoSocial=None)
        
        final_cols = ['DATA','REG_ANS', 'CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
        final_df = df_clean[final_cols].copy()

        # EXPORT
        self._save_to_zip(final_df)

    def _save_to_zip(self, df: pd.DataFrame):
        csv_filename = "consolidado_despesas.csv"
        csv_path = os.path.join(self.output_dir, csv_filename)
        zip_filename = os.path.join(self.output_dir, "consolidado_despesas.zip")

        logger.info(f"Saving final file to {zip_filename}...")
        
        # Save CSV
        df.to_csv(csv_path, index=False, sep=config.CSV_SEP, encoding=config.CSV_ENCODING)
        
        # Zip it
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=csv_filename)
            
        # Cleanup
        os.remove(csv_path)
        logger.info("Success! Consolidation finished.")