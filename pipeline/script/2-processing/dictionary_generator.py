import pandas as pd
import os
import requests
import json

# Percorsi relativi per compatibilità con GitHub
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
percorso_excel = os.path.join(cartella_corrente, "..", "..", "data", "1-preprocessing", "catalogo.xlsx")
percorso_output = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

# Configurazione Ollama
MODELLO_OLLAMA = "llama3.1:latest"
URL_OLLAMA = "http://localhost:11434/api/generate"

def chiedi_a_ollama(parametro, unita_misura):
    """Invia una richiesta al modello locale Ollama per generare la descrizione."""
    
    testo_unita = ""
    if unita_misura != "":
         testo_unita = f"L'unità di misura utilizzata in catalogo per questo parametro è: {unita_misura}."

    prompt_sistema = f"""Sei un ingegnere esperto di sistemi di refrigerazione e HVAC. 
Sto creando un dizionario tecnico per spiegare ai clienti le colonne di un catalogo di macchinari per la refrigerazione.
Scrivi una descrizione tecnica chiara, concisa (massimo 3 frasi) e in italiano del seguente parametro tecnico.
Non usare introduzioni come 'Ecco la descrizione'. Scrivi solo la spiegazione.

Parametro da spiegare: {parametro}
{testo_unita}"""

    payload = {
        "model": MODELLO_OLLAMA,
        "prompt": prompt_sistema,
        "stream": False
    }

    try:
        risposta = requests.post(URL_OLLAMA, json=payload)
        if risposta.status_code == 200:
            dati = risposta.json()
            return dati.get("response", "").strip()
        else:
            return f"[Errore di generazione: codice {risposta.status_code}]"
    except Exception as e:
        return f"[Errore di connessione a Ollama: {e}]"


def genera_dizionario_ai():
    print(f"Lettura del catalogo Excel in corso... ({percorso_excel})")
    
    try:
        # Leggo le prime due righe per avere le intestazioni e i dati per estrarre le unità di misura
        df = pd.read_excel(percorso_excel, nrows=1)
    except Exception as e:
        print(f"Errore nella lettura dell'Excel: {e}")
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

    print(f"Trovati {len(parametri_da_descrivere)} parametri da analizzare. Avvio Llama 3.1...")
    print("Questa operazione richiederà del tempo. Attendi...")

    testo_finale = "DIZIONARIO DEL CATALOGO HVAC\nDi seguito l'elenco dei parametri tecnici tracciati nel database e il loro significato:\n\n"

    # Secondo ciclo: interrogo l'AI per ogni parametro
    contatore = 1
    for parametro, unita in parametri_da_descrivere.items():
        print(f"[{contatore}/{len(parametri_da_descrivere)}] Generazione descrizione per: {parametro}...")
        
        descrizione_ai = chiedi_a_ollama(parametro, unita)
        
        testo_finale = testo_finale + f"**{parametro}**"
        if unita != "" and unita != "nan":
            testo_finale = testo_finale + f" [{unita}]"
        testo_finale = testo_finale + f":\n{descrizione_ai}\n\n"
        
        contatore = contatore + 1

    # Salvataggio del file
    try:
        with open(percorso_output, 'w', encoding='utf-8') as f:
            f.write(testo_finale)
        print(f"\nOperazione completata! Il dizionario intelligente è stato salvato in:\n{percorso_output}")
    except Exception as e:
        print(f"Errore nel salvataggio del file: {e}")

if __name__ == "__main__":
    genera_dizionario_ai()
import pandas as pd
import os

# Prende la cartella in cui si trova QUESTO script
cartella_corrente = os.path.dirname(os.path.abspath(__file__))

# Costruisce il percorso per l'Excel
percorso_excel = os.path.join(cartella_corrente, "..", "..", "data", "1-preprocessing", "catalogo.xlsx")

# Costruisce il percorso di salvataggio del file di testo
percorso_output = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

def crea_scheletro_dizionario():
    print("Lettura del catalogo Excel in corso...")
    
    try:
        # Legge solo l'intestazione per essere velocissimo
        df = pd.read_excel(percorso_excel, nrows=0)
    except Exception as e:
        print(f"Errore nella lettura dell'Excel: {e}")
        return

    # Prende tutte le colonne saltando le prime 3
    colonne_utili = df.columns[3:]

    # Crea il contenuto del file di testo
    testo_dizionario = "DIZIONARIO DEL CATALOGO HVAC\nDi seguito l'elenco dei parametri tecnici tracciati nel database e il loro significato:\n\n"
    
    for colonna in colonne_utili:
        # Pulisce eventuali spazi extra nel nome della colonna
        nome_pulito = str(colonna).strip()
        testo_dizionario += f"{nome_pulito}: [Il parametro indica...]\n\n"

    # Salva il file nella cartella di destinazione
    try:
        with open(percorso_output, 'w', encoding='utf-8') as f:
            f.write(testo_dizionario)
        print(f"Successo! File salvato in: {percorso_output}")
        print("Apri il file generato e sostituisci le parentesi quadre con una breve descrizione per l'AI.")
    except Exception as e:
        print(f"Errore nel salvataggio del file: {e}")

if __name__ == "__main__":
    crea_scheletro_dizionario()