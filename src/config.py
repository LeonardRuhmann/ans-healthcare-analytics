import os

# --- Caminhos ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

DOWNLOAD_DIR = os.path.join(PROJECT_ROOT, "downloads")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# --- URLs ---
ANS_BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
CADASTRE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"

# --- Processamento ---
CSV_SEP = ";"
CSV_ENCODING = "utf-8"

CONSOLIDATED_FILE = "consolidado_despesas.zip"

