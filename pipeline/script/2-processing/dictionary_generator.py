import pandas as pd
import os
from langchain_community.chat_models import ChatOllama
from langchain_core.messages import HumanMessage

def crea_dizionario_con_ai():
    cartella_corrente = os.path.dirname(os.path.abspath(__file__))
    percorso_excel = os.path.join(cartella_corrente, "..", "..", "data", "1-preprocessing", "catalogo.xlsx")
    percorso_output = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

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