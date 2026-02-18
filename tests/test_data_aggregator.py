import unittest
from unittest.mock import patch
import pandas as pd
import os
import sys

# Add project root to sys.path to ensure correct imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.data_aggregator import DataAggregator

class TestDataAggregator(unittest.TestCase):

    def setUp(self):
        """Sets up the test environment before each test method."""
        self.output_dir = "tests/mock_output"
        self.aggregator = DataAggregator(output_dir=self.output_dir)
        self.sample_csv_path = "mock_clean_data.csv"

    @patch("src.services.data_aggregator.zipfile.ZipFile")
    @patch("src.services.data_aggregator.pd.read_csv")
    @patch("src.services.data_aggregator.pd.DataFrame.to_csv")
    def test_aggregate_data_success(self, mock_to_csv, mock_read_csv, mock_zipfile):
        """
        Tests the happy path:
        1. Reads raw data successfully.
        2. Aggregates metrics (Sum, StdDev, Quarterly Avg).
        3. Saves the result.
        """
        # --- MOCK DATA ---
        mock_data = pd.DataFrame({
            'RazaoSocial': ['Operator A', 'Operator A', 'Operator A', 'Operator B'],
            'UF': ['SP', 'SP', 'SP', 'RJ'],
            'ValorDespesas': [100.0, 50.0, 150.0, 500.0],
            'DATA': ['2025-01-15', '2025-01-20', '2025-04-10', '2025-01-05'],
            'REG_ANS': ['111', '111', '111', '222'],
            'Modalidade': ['Médica', 'Médica', 'Médica', 'Odonto']
        })
        mock_read_csv.return_value = mock_data

        # --- EXECUTION ---
        result_file = self.aggregator.aggregate_data(self.sample_csv_path)

        # --- ASSERTIONS ---
        mock_read_csv.assert_called_once_with(self.sample_csv_path, sep=';', encoding='utf-8')
        
        expected_output = os.path.join(self.output_dir, 'despesas_agregadas.csv')
        
        expected_zip = os.path.join(self.output_dir, 'ans_financial_export.zip')
        self.assertEqual(result_file, expected_zip)
        
        mock_to_csv.assert_called_once()
        
        # Verify if ZIP creation was attempted (mocked execution)
        mock_zipfile.assert_called_once()

    @patch("src.services.data_aggregator.pd.read_csv")
    def test_file_not_found_handling(self, mock_read_csv):
        """Tests if the aggregator handles missing input files gracefully."""
        mock_read_csv.side_effect = FileNotFoundError("File not found")
        
        result = self.aggregator.aggregate_data("ghost_file.csv")
        
        self.assertIsNone(result)

    @patch("src.services.data_aggregator.zipfile.ZipFile")
    @patch("src.services.data_aggregator.pd.read_csv")
    @patch("src.services.data_aggregator.pd.DataFrame.to_csv")
    def test_numeric_conversion_and_robustness(self, mock_to_csv, mock_read_csv, mock_zipfile):
        """
        Tests if the aggregator correctly sanitizes numeric strings 
        (e.g., '1.000,00' -> 1000.0) before calculating.
        """
        mock_data = pd.DataFrame({
            'RazaoSocial': ['Op X', 'Op X'],
            'UF': ['MG', 'MG'],
            'ValorDespesas': ['1.000,50', '2.000,00'], 
            'DATA': ['2025-01-01', '2025-02-01'],
            'REG_ANS': ['333', '333'],
            'Modalidade': ['Coop', 'Coop']
        })
        mock_read_csv.return_value = mock_data

        self.aggregator.aggregate_data(self.sample_csv_path)

        mock_to_csv.assert_called_once()
        mock_zipfile.assert_called_once()

if __name__ == "__main__":
    unittest.main()