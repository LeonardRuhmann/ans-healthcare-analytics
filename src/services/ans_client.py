import shutil
import requests
from bs4 import BeautifulSoup
import re
import os
from typing import List, Dict

class AnsDataClient:
    BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

    def __init__(self, download_dir="downloads"):
        self.download_dir = download_dir
        os.makedirs(download_dir, exist_ok=True)

    def _get_links(self, url: str) -> List[str]:
        """Helper to get all href links from a page."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            # Extract hrefs, ignoring 'Parent Directory'
            links = [a.get('href') for a in soup.find_all('a') if a.get('href') and 'Parent' not in a.text]
            return links
        except Exception as e:
            print(f"Error accessing {url}: {e}")
            return []

    def _detect_quarter(self, text: str) -> int:
        """
        Uses Regex to find 1T, 2T, 3T, 4T in filenames.
        - (?<!\d): Ensures the number is NOT preceded by another digit (avoids 2011 -> Q1).
        - ([1-4]): Captures the quarter number.
        - (?: ... ): Non-capturing group for the suffix.
        - [\s\-_]?: Optional separator (matches '1_trim', '1-trim', '1trim').
        - (?: ... ): Alternatives for the indicator.
        - t(?=\d|): Matches 't' or 'T' if followed by a digit (1T2023) OR end of word (1t).
        - tri(?:m(?:estre)?)?: Matches 'tri', 'trim', 'trimestre'.
        - º: Matches '1º'.
        - st|nd|rd|th: Matches English suffixes.
        """

        pattern = re.compile(r'(?<!\d)([1-4])(?:[\s\-_]?(?:t(?=\d|\b)|tri(?:m(?:estre)?)?|º|st|nd|rd|th))',re.IGNORECASE)
        match = re.search(pattern, text)

        if match:
            return int(match.group(1))
        else: 
            return 0



    def get_available_quarters(self):
        """
        Scans years and files to build a list of all available data.
        Returns sorted list: Newest first.
        """
        print(f"Scanning {self.BASE_URL}...")
        root_links = self._get_links(self.BASE_URL)
        print(root_links)

        found_files = []

        year_folders = []
        for link in root_links:
            clean_name = link.strip('/')
            if clean_name.isdigit() and len(clean_name) == 4:
                year_folders.append(clean_name)

        year_folders.sort(reverse=True)

        print(year_folders)
        
        for year in year_folders:
            year_url = f"{self.BASE_URL}{year}/"
            # Get all files inside that year
            files_in_year = self._get_links(year_url)
        
            for file_link in files_in_year:
                if file_link.lower().endswith('.zip'):
                    quarter = self._detect_quarter(file_link)

                    if quarter > 0:
                        found_files.append({
                            "year": int(year),
                            "quarter": quarter,
                            "filename": file_link,
                            "url": f"{year_url}{file_link}"
                        })
        
        # Sort by Year (Descending) first, then Quarter (Descending).
        found_files.sort(key=lambda x: (x['year'], x['quarter']), reverse=True)
        return found_files

    def _download_file(self, url: str, filename: str):
        """
        INTERNAL: Downloads a file using streams (Memory Efficient).
        """
        local_path = os.path.join(self.download_dir, filename)

        if os.path.exists(local_path):
            print(f"File {filename} already exists. Skipping...")
            return
        
        print(f"Downloading {filename}...")
        try:
            # stream=True tells requests NOT to download everything at once
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    # shutil.copyfileobj writes chunks of data to disk as they arrive
                    shutil.copyfileobj(r.raw, f)
            print(f"Success {local_path}")
        except Exception as e:
            print(f"Failed to download {url}: {e}")
    
    def download_last_3_quarters(self):
        """
        Main method to orchestrate the process.
        """
        #  Get the list
        candidates = self.get_available_quarters()
        
        if not candidates:
            print("No files found.")
            return

        print(candidates)

        # Slice the top 3 (since it's already sorted)
        targets = candidates[:3]

        print(f"Targeting top 3 files:")
        for t in targets:
            print(f" - {t['year']} Q{t['quarter']}: {t['filename']}")

        # Download them
        for item in targets:
            self._download_file(item['url'], item['filename'])

    def download_cadastral_data(self):
        """
        Downloads the Active Operators Cadastre (Relatorio_Cadop).
        Target URL: https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv
        """
        base_url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
        filename = "Relatorio_Cadop.csv"
        
        self._download_file(base_url, filename)
        return os.path.join(self.download_dir, filename)