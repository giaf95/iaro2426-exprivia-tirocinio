import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NOME_CATALOGO = os.getenv("NOME_FILE_CATALOGO", "catalogo.xlsx")

CATALOGO_PATH = os.path.join(DATA_DIR, '1-preprocessing', NOME_CATALOGO)
DB_CHAT_PATH = os.path.join(DATA_DIR, '3-user_interface', 'database_chat.db')
CSV_GRAFICI_PATH = os.path.join(DATA_DIR, '3-user_interface', 'dataframe_grafico.csv')
DIR_GRAFICI_SALVATI = os.path.join(DATA_DIR, '3-user_interface', 'grafici_salvati')