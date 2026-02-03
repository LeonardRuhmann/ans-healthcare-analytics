import os
import pandas as pd
from src.services.ans_client import AnsDataClient
from src.services.zip_processor import ZipProcessor
from src.services.data_consolidator import DataConsolidator
from src.services.data_validator import DataValidator
from src.services.data_enricher import DataEnricher

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
    input_for_enricher = "output/consolidado_despesas.zip" 
    
    if os.path.exists(input_for_enricher):
        # This generates 'output/enriched_data.zip'
        enriched_path = enricher.enrich_data(input_for_enricher, cadastral_path)
    else:
        print("Error: Consolidated file not found.")
        return

    # --- VALIDATION ---
    print("\n--- Data Validation ---")
    validator = DataValidator(output_dir='output')
   
    if enriched_path and os.path.exists(enriched_path):
        validator.validate_and_split(enriched_path)
    else:
        print("Error: Enriched file not found.")

if __name__ == "__main__":
    main()