import os
import sys
import logging
from src.services.ans_client import AnsDataClient
from src.services.ingestion import IngestionService
from src.services.data_consolidator import DataConsolidator
from src.services.data_validator import DataValidator
from src.services.data_enricher import DataEnricher
from src.services.data_aggregator import DataAggregator
from src import config

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            # logging.FileHandler("etl_pipeline.log") # Optional: saved to file
        ]
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("--- STARTING ETL PIPELINE ---")
    
    # 1. DOWNLOAD
    client = AnsDataClient()
    downloaded_files = client.download_last_3_quarters()
    
    if not downloaded_files:
        logger.error("No files downloaded. Aborting.")
        return

    # 2. INGESTION (Processing + Concatenation)
    ingestion = IngestionService()
    full_df = ingestion.ingest_from_files(downloaded_files)
    
    if full_df is None or full_df.empty:
        logger.error("No data available after ingestion. Aborting.")
        return

    # 3. CONSOLIDATION
    logger.info("\n--- AGGREGATING DATA ---")
    consolidator = DataConsolidator()
    consolidator.consolidate(full_df)

    # 4. ENRICHMENT
    logger.info("\n--- Data Enrichment (Cadastral Join) ---")
    
    logger.info("Downloading Cadastral Data...")
    cadastral_path = client.download_cadastral_data()
    
    enricher = DataEnricher()
    input_for_enricher = os.path.join(config.OUTPUT_DIR, config.CONSOLIDATED_FILE)
    
    if not os.path.exists(input_for_enricher):
        logger.error("Consolidated file not found. Aborting.")
        return

    enriched_path = enricher.enrich_data(input_for_enricher, cadastral_path)

    # 5. VALIDATION
    logger.info("\n--- Data Validation ---")
    validator = DataValidator(output_dir=config.OUTPUT_DIR)
    clean_path, quarantine_path = validator.validate_and_split(enriched_path)
    
    # 6. AGGREGATION
    logger.info("\n--- Aggregation Strategy ---")
    aggregator = DataAggregator()
    
    if clean_path and os.path.exists(clean_path):
        aggregator.aggregate_data(clean_path)
    else:
        logger.error("No clean data available for aggregation.")

if __name__ == "__main__":
    main()