import pandas as pd
import os
import sys

# Percorsi dinamici tramite config.py
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
cartella_script = os.path.abspath(os.path.join(cartella_corrente, "..", "..", "pipeline", "script"))
sys.path.insert(0, cartella_script)

from config import CATALOGO_PATH, DATA_DIR, GOOGLE_API_KEY
from langchain_google_genai import ChatGoogleGenerativeAI

percorso_catalogo = CATALOGO_PATH
percorso_output = os.path.join(DATA_DIR, "3-user_interface", "dizionario_catalogo.txt")

# Configurazione Gemini
llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash",
    temperature=0,
    google_api_key=GOOGLE_API_KEY
)

def chiedi_a_gemini(parametro, unita_misura):
    """Invia una richiesta al modello Gemini per generare la descrizione."""
    
    testo_unita = ""
    if unita_misura != "":
         testo_unita = f"L'unità di misura utilizzata in catalogo per questo parametro è: {unita_misura}."

    prompt_sistema = f"""Sei un ingegnere esperto di sistemi di refrigerazione e HVAC. 
Sto creando un dizionario tecnico per spiegare ai clienti le colonne di un catalogo di macchinari per la refrigerazione.
Scrivi una descrizione tecnica chiara, concisa (massimo 3 frasi) e in italiano del seguente parametro tecnico.
Non usare introduzioni come 'Ecco la descrizione'. Scrivi solo la spiegazione.

Parametro da spiegare: {parametro}
{testo_unita}"""

    try:
        risposta = llm.invoke(prompt_sistema)
        return risposta.content.strip()
    except Exception as e:
        return f"[Errore di connessione a Gemini: {e}]"


def genera_dizionario_ai():
    print(f"Lettura del catalogo in corso... ({percorso_catalogo})")
    
    try:
        # Lettura robusta compatibile con il CSV pulito e il vecchio Excel
        if percorso_catalogo.lower().endswith(".xlsx"):
            df = pd.read_excel(percorso_catalogo)
        else:
            try:
                df = pd.read_csv(percorso_catalogo, sep=None, engine="python", encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(percorso_catalogo, sep=None, engine="python", encoding="latin-1")
    except Exception as e:
        print(f"Errore nella lettura del catalogo: {e}")
        return

    colonne_totali = df.columns
    colonne_utili = colonne_totali[3:]

    # Dizionario per accoppiare il parametro alla sua unità di misura
    parametri_da_descrivere = {}

    # Primo ciclo: individuo i parametri base e le loro unità di misura
    for colonna in colonne_utili:
        col_str = str(colonna).strip()
        col_str_lower = col_str.lower()
        
        # Se la colonna è una colonna "unit", cerco il suo valore nella prima riga
        if col_str_lower.endswith("unit"):
            nome_parametro_base = col_str[:-4].strip() # tolgo la parola "unit"
            valore_unita = str(df.iloc[0].get(colonna, "")).strip()
            
            # Se il parametro base esiste già nel mio dizionario, gli associo l'unità
            if nome_parametro_base in parametri_da_descrivere:
                parametri_da_descrivere[nome_parametro_base] = valore_unita
        else:
            # È un parametro normale, lo inizializzo con unità di misura vuota
            if col_str not in parametri_da_descrivere:
                parametri_da_descrivere[col_str] = ""

    print(f"Trovati {len(parametri_da_descrivere)} parametri da analizzare. Avvio Gemini...")
    print("Questa operazione richiederà del tempo. Attendi...")

    testo_finale = "DIZIONARIO DEL CATALOGO HVAC\nDi seguito l'elenco dei parametri tecnici tracciati nel database e il loro significato:\n\n"

    # Secondo ciclo: interrogo l'AI per ogni parametro
    contatore = 1
    for parametro, unita in parametri_da_descrivere.items():
        print(f"[{contatore}/{len(parametri_da_descrivere)}] Generazione descrizione per: {parametro}...")
        
        descrizione_ai = chiedi_a_gemini(parametro, unita)
        
        testo_finale = testo_finale + f"**{parametro}**"
        if unita != "" and unita != "nan":
            testo_finale = testo_finale + f" [{unita}]"
        testo_finale = testo_finale + f":\n{descrizione_ai}\n\n"
        
        contatore = contatore + 1

    # Salvataggio del file
    os.makedirs(os.path.dirname(percorso_output), exist_ok=True)
    try:
        with open(percorso_output, 'w', encoding='utf-8') as f:
            f.write(testo_finale)
        print(f"\nOperazione completata! Il dizionario intelligente è stato salvato in:\n{percorso_output}")
    except Exception as e:
        print(f"Errore nel salvataggio del file: {e}")

if __name__ == "__main__":
    genera_dizionario_ai()