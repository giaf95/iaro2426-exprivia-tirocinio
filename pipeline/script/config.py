import os
from dotenv import load_dotenv

# 1. Calcoliamo i percorsi principali
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))       # .../pipeline/script
PIPELINE_DIR = os.path.dirname(CONFIG_DIR)                    # .../pipeline
ROOT_DIR = os.path.dirname(PIPELINE_DIR)                      # .../cartella_principale (dove sta .env)

# 2. Carichiamo il file .env dicendogli ESATTAMENTE dove si trova
ENV_PATH = os.path.join(ROOT_DIR, '.env')
load_dotenv(dotenv_path=ENV_PATH)

# 3. Definiamo la cartella data (che prima mancava e causava il crash)
DATA_DIR = os.path.join(PIPELINE_DIR, 'data')

# 4. Variabili d'ambiente
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NOME_CATALOGO = os.getenv("NOME_FILE_CATALOGO", "catalogo_sintetico_completo.csv")

# 5. Percorsi centralizzati
CATALOGO_PATH = os.path.join(DATA_DIR, '1-preprocessing', NOME_CATALOGO)
DB_CHAT_PATH = os.path.join(DATA_DIR, '3-user_interface', 'database_chat.db')
CSV_GRAFICI_PATH = os.path.join(DATA_DIR, '3-user_interface', 'dataframe_grafico.csv')
DIR_GRAFICI_SALVATI = os.path.join(DATA_DIR, '3-user_interface', 'grafici_salvati')

# Controlliamo se dotenv ha caricato il file
esito_caricamento = load_dotenv(dotenv_path=ENV_PATH)

if not GOOGLE_API_KEY:
    print(f"\n[ALLARME ROSSO] Chiave API non trovata!")
    print(f"Esito lettura file .env: {esito_caricamento}")
    print(f"Percorso: {ENV_PATH}\n")