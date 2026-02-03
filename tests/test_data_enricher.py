import pytest
import pandas as pd
import os
import sys
import zipfile

# Path Setup
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.data_enricher import DataEnricher

@pytest.fixture
def mock_financial_zip(tmp_path):
    """Creates a fake Financial Data ZIP (Left Table)."""
    # Note: CNPJ and RazaoSocial are empty initially
    csv_content = """DATA;REG_ANS;CD_CONTA_CONTABIL;DESCRICAO;ValorDespesas;CNPJ;RazaoSocial
2025-01-01;111111;1001;Despesa A;500.0;;
2025-01-01;222222;1001;Despesa B;600.0;;
2025-01-01;999999;1001;Ghost Company;700.0;;"""
    
    zip_path = tmp_path / "financial.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("financial.csv", csv_content.encode('utf-8'))
    return str(zip_path)

@pytest.fixture
def mock_cadastral_csv(tmp_path):
    """Creates a fake Cadastral CSV (Right Table)."""
    # Note: 111111 and 222222 exist. 999999 is missing.
    csv_content = """REGISTRO_OPERADORA;CNPJ;Razao_Social;Modalidade;UF
111111;12345678000199;Empresa Teste Um;Medicina de Grupo;SP
222222;87654321000155;Empresa Teste Dois;Seguradora;RJ"""
    
    csv_path = tmp_path / "cadastral.csv"
    # Using UTF-8 as verified by you
    with open(csv_path, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    return str(csv_path)

def test_enrichment_logic(mock_financial_zip, mock_cadastral_csv, tmp_path):
    """
    Test if the enricher:
    1. Joins correctly on REG_ANS
    2. Fills CNPJ and RazaoSocial
    3. Handles missing keys (Left Join)
    """
    enricher = DataEnricher(output_dir=str(tmp_path))
    
    # Run
    output_file = enricher.enrich_data(mock_financial_zip, mock_cadastral_csv)
    
    # Verify File Creation
    assert os.path.exists(output_file)
    
    # Verify Content
    df = pd.read_csv(output_file, sep=';', compression='zip', dtype=str)
    
    # CASE 1: Successful Match (REG_ANS 111111)
    # It should have enriched data from the Cadastre
    row_1 = df[df['REG_ANS'] == '111111'].iloc[0]
    assert row_1['CNPJ'] == '12345678000199'
    assert row_1['RazaoSocial'] == 'Empresa Teste Um'
    assert row_1['UF'] == 'SP'
    
    # CASE 2: No Match (REG_ANS 999999)
    # Should keep the row (Left Join) but fill metadata with defaults
    row_ghost = df[df['REG_ANS'] == '999999'].iloc[0]
    assert pd.isna(row_ghost['CNPJ']) or row_ghost['CNPJ'] == '' 
    assert row_ghost['Modalidade'] == 'DESCONHECIDO'
    assert row_ghost['UF'] == 'ND'
    
    # Verify Total Rows (Should match Financial Input = 3)
    assert len(df) == 3