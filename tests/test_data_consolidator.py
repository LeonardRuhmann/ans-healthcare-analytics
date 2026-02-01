import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.data_consolidator import DataConsolidator

# Instead of downloading real files, we create a small DataFrame in memory
# containing exactly the problems we want to test.
@pytest.fixture
def mock_dirty_data():
    return pd.DataFrame({
        'REG_ANS': ['111111', '111111', '111111', '222222'],
        'DATA': [
            '2025-01-01',  # Good Date
            '2025-01-01',  # Good Date (Duplicate Candidate)
            '01/01/2025',  # Bad Format (Needs normalization)
            '2025-02-01'   # Good Date
        ],
        'CD_CONTA_CONTABIL': ['1001', '1001', '1001', '2002'],
        'ValorDespesas': [
            500.0,   # Row 1: Normal
            500.0,   # Row 2: Exact Duplicate of Row 1 (Same ID, Date, Account, Value)
            0.0,     # Row 3: Zero (Should be removed)
            -100.0   # Row 4: Negative (Should be KEPT - Reversal)
        ],
        'DESCRICAO': ['Expense A', 'Expense A', 'Expense Zero', 'Reversal']
    })

#  This function runs the logic and checks the results
def test_consolidator_logic(mock_dirty_data, tmp_path):
    # tmp_path is a pytest feature that creates a temporary folder that deletes itself later.
    # We use it so we don't pollute your real 'output/' folder.
    output_dir = tmp_path / "test_output"
    consolidator = DataConsolidator(output_dir=str(output_dir))

    # We pass our fake data to your real class
    consolidator.consolidate(mock_dirty_data)
    
    # Check 1: Did it create the ZIP file?
    expected_zip = output_dir / "consolidado_despesas.zip"
    assert expected_zip.exists()

    # Check 2: Open the ZIP and read the CSV inside to inspect the rows
    import zipfile
    with zipfile.ZipFile(expected_zip, 'r') as z:
        # We assume the file inside has this specific name
        with z.open("consolidado_despesas.csv") as f:
            result_df = pd.read_csv(f, sep=';')

    # --- BUSINESS RULE CHECKS ---

    # Rule: Zeros must be gone
    # Our input had one row with 0.0. The result should have 0.
    assert len(result_df[result_df['ValorDespesas'] == 0]) == 0

    # Rule: Negatives must stay
    # Our input had one row with -100.0. It must still be there.
    assert len(result_df[result_df['ValorDespesas'] < 0]) == 1

    # Rule: Duplicates must be gone
    # Row 1 and Row 2 were identical. Only one should remain.
    # Total Expected Rows: 2 (The 500.0 and the -100.0). The 0.0 was removed.
    assert len(result_df) == 2

    # Rule: Dates must be normalized
    # '2025-01-01' and '01/01/2025' should both end up in Year 2025
    assert all(result_df['Ano'] == 2025)
    
    # Rule: Check if the extra columns were added (even if empty)
    assert 'CNPJ' in result_df.columns
    assert 'RazaoSocial' in result_df.columns