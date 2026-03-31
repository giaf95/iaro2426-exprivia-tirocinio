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
        df = pd.read_excel(percorso_excel, nrows=0)
    except Exception as e:
        print(f"Errore nella lettura dell'Excel: {e}")
        return

    colonne_utili = df.columns[3:]
    
    # inizializza il modello pesante per la generazione offline
    print("Inizializzazione di Llama 3.1 in corso...")
    llm = ChatOllama(model="llama3.1:8b-instruct-q4_K_M", temperature=0.1)

    testo_dizionario = "DIZIONARIO DEL CATALOGO HVAC\nDi seguito l'elenco dei parametri tecnici tracciati nel database e il loro significato:\n\n"

    print(f"Trovate {len(colonne_utili)} colonne. Avvio generazione AI...")
    print("Mettiti comodo, potrebbe volerci qualche minuto a seconda della potenza del tuo i5.\n")

    # ciclo for per interrogare l'AI per ogni colonna
    for colonna in colonne_utili:
        nome_pulito = str(colonna).strip()
        print(f"- Generazione definizione per: {nome_pulito}...")
        
        # prompt molto restrittivo per evitare che l'AI scriva poemi
        prompt = f"Sei un ingegnere termotecnico esperto (HVAC). Spiega in italiano, in massimo 2 frasi, cosa indica il parametro '{nome_pulito}' in un catalogo di macchinari per il condizionamento dell'aria. Rispondi SOLO con la spiegazione tecnica, senza altre parole."
        
        try:
            risposta = llm.invoke([HumanMessage(content=prompt)])
            spiegazione = risposta.content.strip()
        except Exception as e:
            spiegazione = "Descrizione non generata a causa di un errore dell'AI."
            print(f"  Errore: {e}")
            
        testo_dizionario = testo_dizionario + f"{nome_pulito}: {spiegazione}\n\n"

    # salva il file
    try:
        with open(percorso_output, 'w', encoding='utf-8') as f:
            f.write(testo_dizionario)
        print(f"\nSuccesso! File generato dall'AI e salvato in: {percorso_output}")
    except Exception as e:
        print(f"Errore nel salvataggio del file: {e}")

if __name__ == "__main__":
    crea_dizionario_con_ai()