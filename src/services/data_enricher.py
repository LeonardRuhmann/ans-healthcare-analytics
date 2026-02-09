import pandas as pd
import os

import logging
import pandas as pd
import os

logger = logging.getLogger(__name__)

class DataEnricher:
    def __init__(self, output_dir='output'):
        self.output_dir = output_dir

    def _load_cadastral_csv(self, path: str) -> pd.DataFrame:
        """
        Helper to load the Cadastral CSV handling Encoding issues automatically.
        """
        # We only need these columns to enrich our data
        cols = ['REGISTRO_OPERADORA', 'CNPJ', 'Razao_Social', 'Modalidade', 'UF']
        
        try:
            # Try UTF-8 first
            return pd.read_csv(path, sep=';', encoding='utf-8', usecols=cols, dtype=str)
        except (UnicodeDecodeError, ValueError):
            logger.warning("   UTF-8 failed. Retrying with Latin-1...")
            return pd.read_csv(path, sep=';', encoding='latin-1', usecols=cols, dtype=str)

    def enrich_data(self, financial_zip_path: str, cadastral_csv_path: str):
        logger.info(f"    Loading Financial Data: {financial_zip_path}...")
        df_fin = pd.read_csv(financial_zip_path, compression='zip', sep=';', encoding='utf-8', dtype=str)
        
        logger.info(f"   Loading Cadastral Data: {cadastral_csv_path}...")
        df_cad = self._load_cadastral_csv(cadastral_csv_path)

        # STANDARDIZE KEYS (Remove spaces, ensure string)
        df_fin['REG_ANS'] = df_fin['REG_ANS'].str.strip()
        df_cad['REGISTRO_OPERADORA'] = df_cad['REGISTRO_OPERADORA'].str.strip()

        # DEDUPLICATE CADASTRE
        # Trade-off: If ID duplicates exist, keep the first one to avoid row explosion
        df_cad = df_cad.drop_duplicates(subset=['REGISTRO_OPERADORA'])

        # PERFORM JOIN (Left Join)
        logger.info(f"   Merging {len(df_fin)} rows...")
        merged_df = pd.merge(
            df_fin, 
            df_cad, 
            left_on='REG_ANS', 
            right_on='REGISTRO_OPERADORA', 
            how='left'
        )

        # FILL MISSING VALUES (The Enrichment Step)
        # We fill the empty CNPJ/Razao from the financial file with the data from Cadastre
        merged_df['CNPJ'] = merged_df['CNPJ_y'].fillna(merged_df['CNPJ_x'])
        merged_df['RazaoSocial'] = merged_df['Razao_Social'].fillna(merged_df['RazaoSocial'])
        
        # HANDLE NO MATCHES (Trade-off: Unknown vs Drop)
        # We choose to keep the data (Unknown)
        merged_df['Modalidade'] = merged_df['Modalidade'].fillna('DESCONHECIDO')
        merged_df['UF'] = merged_df['UF'].fillna('ND')

        # 1. Convert to String
        # 2. Remove ".0" if it exists (float artifact)
        # 3. Fill "nan" strings back to real None (so we can filter them)
        # 4. Pad with Zeros to ensure 14 digits (Fixes the Sul America bug)
        merged_df['CNPJ'] = merged_df['CNPJ'].astype(str).str.replace(r'\.0$', '', regex=True)
        merged_df['CNPJ'] = merged_df['CNPJ'].replace('nan', '') # Handle string 'nan'

        # It turns "1685053000156" into "01685053000156"
        mask_valid = merged_df['CNPJ'] != ''
        merged_df.loc[mask_valid, 'CNPJ'] = merged_df.loc[mask_valid, 'CNPJ'].str.zfill(14)

        # SELECT FINAL COLUMNS
        final_cols = [
            'DATA', 'REG_ANS', 'CD_CONTA_CONTABIL', 'DESCRICAO', 
            'ValorDespesas', 'CNPJ', 'RazaoSocial', 'UF', 'Modalidade'
        ]
        
        # Ensure only existing columns are selected
        available = [c for c in final_cols if c in merged_df.columns]
        final_df = merged_df[available]

        # SAVE
        output_file = os.path.join(self.output_dir, 'enriched_data.zip')
        compression_opts = dict(method='zip', archive_name='enriched_data.csv')
        final_df.to_csv(output_file, index=False, sep=';', compression=compression_opts, encoding='utf-8')
        
        return output_file