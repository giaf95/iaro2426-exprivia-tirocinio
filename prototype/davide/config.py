import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOOGLE_API_KEY = "dummy"
GROQ_API_KEY = "dummy" 
NOME_CSV = "catalogo_sintetico_completo.csv"
CATALOGO_PATH = os.path.join(BASE_DIR, "data", "1-preprocessing", NOME_CSV)
CSV_GRAFICI_PATH = os.path.join(BASE_DIR, "data", "1-preprocessing", "dataframe_grafico.csv")