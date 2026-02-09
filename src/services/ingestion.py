import os
import logging
import pandas as pd
from typing import List, Optional
from src.services.zip_processor import ZipProcessor

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self):
        self.processor = ZipProcessor()

    def ingest_from_files(self, file_paths: List[str]) -> Optional[pd.DataFrame]:
        """
        Iterates over the provided ZIP paths, extracts the relevant CSV/XLSX data,
        and aggregates them into a single DataFrame.
        """
        all_data = []
        logger.info(f"Starting ingestion for {len(file_paths)} files.")

        for zip_file in file_paths:
            logger.info(f"Processing: {zip_file}")
            try:
                df = self.processor.process_zip(zip_file)
                
                if df is not None and not df.empty:
                    # Enrich with source metadata
                    df['SOURCE_FILE'] = os.path.basename(zip_file)
                    all_data.append(df)
            except Exception as e:
                logger.error(f"Failed to ingest {zip_file}: {e}")
                continue
        
        if not all_data:
            logger.warning("No data ingested from any available file.")
            return None
            
        logger.info(f"Aggregating {len(all_data)} DataFrames.")
        try:
            full_df = pd.concat(all_data, ignore_index=True)
            return full_df
        except Exception as e:
            logger.error(f"Failed to concatenate DataFrames: {e}")
            return None
