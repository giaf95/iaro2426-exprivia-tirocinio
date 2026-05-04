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

chiavi_google_aggiuntive = []

for nome_variabile, valore_variabile in os.environ.items():
    if nome_variabile.startswith("GOOGLE_API_KEY_"):
        suffisso = nome_variabile.replace("GOOGLE_API_KEY_", "").strip()
        if suffisso.isdigit() and valore_variabile and valore_variabile.strip():
            chiavi_google_aggiuntive.append((int(suffisso), valore_variabile.strip()))

chiavi_google_aggiuntive.sort(key=lambda x: x[0])

GOOGLE_API_KEYS = []
if GOOGLE_API_KEY and GOOGLE_API_KEY.strip():
    GOOGLE_API_KEYS.append(GOOGLE_API_KEY.strip())

GOOGLE_API_KEYS.extend([valore for _, valore in chiavi_google_aggiuntive])

if not GOOGLE_API_KEYS:
    raise ValueError("Nessuna GOOGLE_API_KEY valida trovata nel file .env")
NOME_CATALOGO = os.getenv("NOME_FILE_CATALOGO", "catalogo_sintetico_completo.csv")

# 5. Percorsi centralizzati
CATALOGO_PATH = os.path.join(DATA_DIR, '1-preprocessing', NOME_CATALOGO)
DB_CHAT_PATH = os.path.join(DATA_DIR, '3-user_interface', 'database_chat.db')
CSV_GRAFICI_PATH = os.path.join(DATA_DIR, '3-user_interface', 'dataframe_grafico.csv')
DIR_GRAFICI_SALVATI = os.path.join(DATA_DIR, '3-user_interface', 'grafici_salvati')