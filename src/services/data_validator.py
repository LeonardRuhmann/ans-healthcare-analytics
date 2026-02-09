import logging
import pandas as pd
import os
from src.utils.validators import validate_cnpj
from src import config

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, output_dir=config.OUTPUT_DIR):
        self.output_dir = output_dir

    def validate_and_split(self, input_zip_path: str):
        """
        Reads the consolidated ZIP, validates business rules,
        and splits data into 'clean' and 'quarantine'.
        """
        logger.info(f"    Reading data from {input_zip_path}...")
        
        # Pandas can read directly from ZIP if it contains one CSV
        try:
            df = pd.read_csv(input_zip_path, compression='zip', sep=config.CSV_SEP, encoding=config.CSV_ENCODING, dtype=str)
        except Exception as e:
            logger.error(f"   [Error] Could not read ZIP file: {e}")
            return None

        logger.info(f"    Validating {len(df)} rows...")

        # Replace comma with dot (standardize)
        df['ValorDespesas'] = df['ValorDespesas'].str.replace(',', '.', regex=False)
        
        # Convert to Number (Coerce errors to NaN, then fill with 0)
        df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce').fillna(0)

        # --- RULES ---
        
        #  CNPJ Validation
        valid_cnpj = df['CNPJ'].apply(validate_cnpj)
        
        # Razao Social (Cannot be Empty)
        valid_razao = df['RazaoSocial'].notna() & (df['RazaoSocial'] != '')
        
        # Positive Values
        valid_value = df['ValorDespesas'] > 0

        # --- FLAGGING ---
        
        df['validation_errors'] = ''
        
        # It uses Vectorized operations (faster than loops) to mark errors
        df.loc[~valid_cnpj, 'validation_errors'] += 'Invalid CNPJ; '
        df.loc[~valid_razao, 'validation_errors'] += 'Missing Razao Social; '
        df.loc[~valid_value, 'validation_errors'] += 'Non-Positive Value; '

        # Define "Clean" as having NO errors
        df['is_valid'] = (df['validation_errors'] == '')

        # --- SPLITTING ---
        clean_df = df[df['is_valid']].copy().drop(columns=['is_valid', 'validation_errors'])
        quarantine_df = df[~df['is_valid']].copy().drop(columns=['is_valid'])

        # --- SAVING ---
        clean_path = os.path.join(self.output_dir, 'data_clean.csv')
        quarantine_path = os.path.join(self.output_dir, 'data_quarantine.csv')

        clean_df.to_csv(clean_path, sep=config.CSV_SEP, index=False, encoding=config.CSV_ENCODING)
        quarantine_df.to_csv(quarantine_path, sep=config.CSV_SEP, index=False, encoding=config.CSV_ENCODING)

        logger.info(f"    Done.")
        logger.info(f"   -> Clean Rows: {len(clean_df)} (Saved to {clean_path})")
        logger.info(f"   -> Quarantine Rows: {len(quarantine_df)} (Saved to {quarantine_path})")
        
        return clean_path, quarantine_path