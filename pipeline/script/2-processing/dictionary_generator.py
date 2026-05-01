import pandas as pd
import os
import sys

# Percorsi dinamici tramite config.py
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
cartella_script = os.path.dirname(cartella_corrente)
sys.path.insert(0, cartella_script)

from config import CATALOGO_PATH, DATA_DIR, GOOGLE_API_KEYS
from langchain_google_genai import ChatGoogleGenerativeAI

percorso_catalogo = CATALOGO_PATH
percorso_output = os.path.join(DATA_DIR, "3-user_interface", "dizionario_catalogo.txt")

# Gestione multi API key per evitare RESOURCE_EXHAUSTED
if not GOOGLE_API_KEYS:
    raise RuntimeError("Nessuna GOOGLE_API_KEY configurata nel file .env")

indice_api_corrente = 0


def ottieni_api_key_corrente() -> str:
    return GOOGLE_API_KEYS[indice_api_corrente]


def passa_alla_prossima_api_key() -> bool:
    global indice_api_corrente
    if indice_api_corrente + 1 >= len(GOOGLE_API_KEYS):
        print("[LLM] Nessun'altra API key disponibile per il dizionario.")
        return False

    indice_api_corrente += 1
    print(
        f"[LLM] Switch API key dizionario -> {indice_api_corrente + 1}/{len(GOOGLE_API_KEYS)}"
    )
    return True


def crea_llm_con_chiave(api_key: str) -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash-lite",
        temperature=0,
        google_api_key=api_key,
    )

# Configurazione Gemini (usa la chiave corrente con rotazione)
llm = crea_llm_con_chiave(ottieni_api_key_corrente())

def chiedi_a_gemini(parametro, unita_misura):
    """Invia una richiesta al modello Gemini per generare la descrizione."""
    global llm

    testo_unita = ""
    if unita_misura != "":
        testo_unita = f"L'unità di misura utilizzata in catalogo per questo parametro è: {unita_misura}."

    prompt_sistema = f"""Sei un ingegnere esperto di sistemi di refrigerazione e HVAC. 
Sto creando un dizionario tecnico per spiegare ai clienti le colonne di un catalogo di macchinari per la refrigerazione.
Scrivi una descrizione tecnica chiara, concisa (massimo 3 frasi) e in italiano del seguente parametro tecnico.
Non usare introduzioni come 'Ecco la descrizione'. Scrivi solo la spiegazione.

Parametro da spiegare: {parametro}
{testo_unita}"""

    tentativi = 0
    max_tentativi = len(GOOGLE_API_KEYS)

    while tentativi < max_tentativi:
        try:
            risposta = llm.invoke(prompt_sistema)
            return risposta.content.strip()
        except Exception as e:
            msg = str(e)
            # Gestione specifica per 429 / RESOURCE_EXHAUSTED
            if "RESOURCE_EXHAUSTED" in msg or "429" in msg:
                print(f"[LLM] Errore 429/RESOURCE_EXHAUSTED: {msg}")
                if not passa_alla_prossima_api_key():
                    return f"[Errore di connessione a Gemini dopo rotazione chiavi: {e}]"
                llm = crea_llm_con_chiave(ottieni_api_key_corrente())
                tentativi += 1
                continue
            return f"[Errore di connessione a Gemini: {e}]"

    return "[Errore di connessione a Gemini: nessuna chiave disponibile senza 429]"


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