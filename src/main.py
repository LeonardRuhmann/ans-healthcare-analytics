import os
import pandas as pd
from src.services.ans_client import AnsDataClient
from src.services.zip_processor import ZipProcessor
from src.services.data_consolidator import DataConsolidator
from src.services.data_validator import DataValidator
from src.services.data_enricher import DataEnricher
from src.services.data_aggregator import DataAggregator
from src import config

def main():
    print("--- STARTING ETL PIPELINE ---")
    
    # Download 
    client = AnsDataClient()
    client.download_last_3_quarters()
    
    # Processing
    processor = ZipProcessor()
    
    # Get list of downloaded files (files ending in .zip)
    downloaded_files = [
        os.path.join(client.download_dir, f) 
        for f in os.listdir(client.download_dir) 
        if f.endswith('.zip')
    ]
    
    all_data = []
    
    print(f"\n--- PROCESSING {len(downloaded_files)} FILES ---")
    
    for zip_file in downloaded_files:
        print(f"Processing: {zip_file}")
        
        # The 'process_zip' method finds the CSV inside automatically
        df = processor.process_zip(zip_file)
        
        if df is not None and not df.empty:
            # Add a column to know which file this came from
            df['SOURCE_FILE'] = os.path.basename(zip_file)
            all_data.append(df)
    
    # Consolidation
    if all_data:
        print("\n--- AGGREGATING DATA ---")
        full_df = pd.concat(all_data, ignore_index=True)
        
        consolidator = DataConsolidator()
        consolidator.consolidate(full_df)

    else:
        print("No data processed.")
        return

    print("\n--- Data Enrichment (Cadastral Join) ---")
    
    # Download Cadastre
    print("Downloading Cadastral Data...")
    cadastral_path = client.download_cadastral_data()
    
    # Run Enricher
    enricher = DataEnricher()
    
    # Input: The output from consolidator
    input_for_enricher = os.path.join(config.OUTPUT_DIR, config.CONSOLIDATED_FILE)
    
    if os.path.exists(input_for_enricher):
        # This generates 'output/enriched_data.zip'
        enriched_path = enricher.enrich_data(input_for_enricher, cadastral_path)
    else:
        print("Error: Consolidated file not found.")
        return

    # --- VALIDATION ---
    print("\n--- Data Validation ---")
    validator = DataValidator(output_dir=config.OUTPUT_DIR)
   
    clean_path, quarantine_path = validator.validate_and_split(enriched_path)
    
    # --- AGGREGATION ---
    print("\n--- Aggregation Strategy ---")
    aggregator = DataAggregator()
    
    if clean_path and os.path.exists(clean_path):
        aggregator.aggregate_data(clean_path)
    else:
        print("   [Error] No clean data available for aggregation.")

if __name__ == "__main__":
    main()