import os
from dotenv import load_dotenv

# 1. Calcoliamo i percorsi principali
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))       
PIPELINE_DIR = os.path.dirname(CONFIG_DIR)                    
ROOT_DIR = os.path.dirname(PIPELINE_DIR)                      

# 2. CARICHIAMO IL FILE .ENV PRIMA DI CHIAMARE LE VARIABILI
ENV_PATH = os.path.join(ROOT_DIR, '.env')
load_dotenv(dotenv_path=ENV_PATH)

# 3. Definiamo la cartella data
DATA_DIR = os.path.join(PIPELINE_DIR, 'data')

# 4. ORA possiamo leggere le variabili d'ambiente in modo sicuro
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_API_KEY_2 = os.getenv("GOOGLE_API_KEY_2")
GOOGLE_API_KEY_3 = os.getenv("GOOGLE_API_KEY_3")
GOOGLE_API_KEY_4 = os.getenv("GOOGLE_API_KEY_4")

GOOGLE_API_KEYS = [
    key for key in [
        GOOGLE_API_KEY,
        GOOGLE_API_KEY_2,
        GOOGLE_API_KEY_3,
        GOOGLE_API_KEY_4
    ]
    if key and key.strip()
]
NOME_CATALOGO = os.getenv("NOME_FILE_CATALOGO", "catalogo_sintetico_completo.csv")

# 5. Percorsi centralizzati
CATALOGO_PATH = os.path.join(DATA_DIR, '1-preprocessing', NOME_CATALOGO)
DB_CHAT_PATH = os.path.join(DATA_DIR, '3-user_interface', 'database_chat.db')
CSV_GRAFICI_PATH = os.path.join(DATA_DIR, '3-user_interface', 'dataframe_grafico.csv')
DIR_GRAFICI_SALVATI = os.path.join(DATA_DIR, '3-user_interface', 'grafici_salvati')