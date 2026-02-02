import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from io import BytesIO
from requests.exceptions import HTTPError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.ans_client import AnsDataClient

def test_directory_creation(tmp_path):
    """Test if the client creates the download folder automatically."""
    assert os.path.exists(tmp_path)

@patch('src.services.ans_client.requests.get')
def test_download_file_success(mock_get, tmp_path):
    """
    Test the download logic with stream=True support.
    """
    
    # When requests.get is called, it will return this object instead of connecting to the web.
    mock_response = MagicMock()
    mock_response.status_code = 200 # HTTP OK
    # Mock r.raw (not iter_content) because the code uses shutil.copyfileobj(r.raw, f)
    mock_response.raw = BytesIO(b"Fake Zip Content")
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_get.return_value = mock_response

    # Setup Client
    client = AnsDataClient(download_dir=str(tmp_path))
    
    # Execution
    # We call the internal download method directly to test it in isolation
    url = "http://fake-url.com/data.zip"
    client._download_file(url, "test_download.zip")
    
    # Verification
    
    # Check if requests.get was actually called with our URL
    mock_get.assert_called_once_with(url, stream=True)
    
    # Check if the file was actually saved to disk
    expected_file = tmp_path / "test_download.zip"
    assert expected_file.exists()
    
    # Check if the content inside the file matches our fake data
    assert expected_file.read_bytes() == b"Fake Zip Content"

@patch('src.services.ans_client.requests.get')
def test_download_failure(mock_get, tmp_path):
    """Test how the code handles a 404 (Not Found) error."""
    # Setup Mock for Failure
    mock_response = MagicMock()
    mock_response.status_code = 404 # Not Found
    # Configure raise_for_status to raise HTTPError (simulating 404 behavior)
    mock_response.raise_for_status.side_effect = HTTPError("404 Client Error: Not Found")
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_get.return_value = mock_response
    
    client = AnsDataClient(download_dir=str(tmp_path))
    
    # Execution
    # We try to download. Since status is 404, raise_for_status will raise and file should NOT be created.
    client._download_file("http://fake-url.com/missing.zip", "missing.zip")
    
    # 3. Verification
    expected_file = tmp_path / "missing.zip"
    # The file should NOT exist
    assert not expected_file.exists()