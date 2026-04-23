import pandas as pd
import numpy as np
import os
from langchain_core.tools import tool
from langchain_ollama import ChatOllama
from typing import TypedDict
from langgraph.graph import StateGraph, START, END

class GraphState(TypedDict):
    percorso_file: str 
    messaggio: str

@tool
def anonimizza_dati(percorso_excel: str) -> str:
    """Legge un file Excel, pulisce i nomi e anonimizza i numeri usando le maschere vettoriali di Pandas."""
    
    df = pd.read_excel(percorso_excel)
    df.columns = df.columns.str.strip()
    
    colonne_trovate = [c for c in df.columns if 'modello' in c.lower() and 'prodotto' in c.lower()]
    nome_colonna_modello = colonne_trovate[0] if colonne_trovate else None
    
    if nome_colonna_modello:
        df[nome_colonna_modello] = ""
    
    for col in df.columns:
        if col == nome_colonna_modello:
            continue
            
        temp_col = df[col].astype(str).str.strip()
        
        temp_col = temp_col.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)

        numeric_col = pd.to_numeric(temp_col, errors='coerce')
        
        mask = numeric_col.notna()
        
        quanti_numeri = mask.sum()
        
        if quanti_numeri > 0:
            moltiplicatori = np.random.uniform(0.90, 1.10, size=quanti_numeri)
            
            nuovi_valori = (numeric_col[mask] * moltiplicatori).round(2)
            df.loc[mask, col] = nuovi_valori
            
    cartella_temp = os.path.dirname(percorso_excel)
    percorso_salvataggio = os.path.join(cartella_temp, 'catalogo_finto.csv')
    
    df.to_csv(percorso_salvataggio, index=False, sep=';', decimal=',')
    
    return percorso_salvataggio


llm = ChatOllama(model="qwen2.5:7b-instruct-q4_K_M", temperature=0.7)

@tool
def crea_nomi(percorso_csv: str) -> str:
    """Legge il catalogo anonimizzato e genera nomi in locale tramite Qwen."""
    df = pd.read_csv(percorso_csv, sep=';')
    
    prompt = """Sei un generatore di codici.
    Devi seguire ESATTAMENTE questa logica basandoti sui 3 esempi:

    ESEMPIO 1
    DATI: Parametro_A = 3200
    RISULTATO: AX-32

    ESEMPIO 2
    DATI: Parametro_A = 4500
    RISULTATO: BF-45

    ESEMPIO 3
    DATI: Parametro_A = 6900
    RISULTATO: CM-69

    REGOLE:
    1. Inventa un prefisso di due lettere casuali.
    2. Unisci con un trattino.
    3. Aggiungi le prime due cifre del "Parametro_A".
    4. Il tuo output deve essere ESCLUSIVAMENTE il nome generato. Niente saluti, niente testo extra."""
    
    nuovi_nomi = []
    
    for _, row in df.iterrows():
        valore_eff = row.get('Efficenza portata', '0000') 
        messaggio_utente = f"DATI: Parametro_A = {valore_eff}"
        
        risposta = llm.invoke([
            ("system", prompt),
            ("human", messaggio_utente)
        ])
        
        nuovi_nomi.append(risposta.content.strip())
        
    df["Modello PAL"] = nuovi_nomi
    percorso_finale = percorso_csv.replace('catalogo_finto.csv', 'catalogo_sintetico_completo.csv')
    df.to_csv(percorso_finale, index=False, sep=';')
    
    return f"Anonimizzazione completata in LOCALE! Catalogo salvato in: {percorso_finale}"

def nodo_matematico(state: GraphState):
    nuovo_percorso = anonimizza_dati.invoke(state['percorso_file'])
    return{
        "percorso_file": nuovo_percorso,
        "messaggio": "Dati anonimizzati matematicamente."
    }

def nodo_nomi(state: GraphState):
    risultato_finale = crea_nomi.invoke(state['percorso_file'])
    return{
        "messaggio": risultato_finale
    }

workflow = StateGraph(GraphState)
workflow.add_node("anonimizzazione", nodo_matematico)
workflow.add_node("generazione_nomi", nodo_nomi)
workflow.add_edge(START, "anonimizzazione")
workflow.add_edge("anonimizzazione", "generazione_nomi")
workflow.add_edge("generazione_nomi", END)
app = workflow.compile()

if __name__ == "__main__":
    
    percorso_vero = r"C:\Users\PC_A87\Desktop\Carricamento Progetti GIT\pipeline\data\1-preprocessing\catalogo.xlsx"
    
    stato_iniziale = {
        "percorso_file": percorso_vero,
        "messaggio": "Avvio pipeline locale"
    }
    
    print("Avvio del processo locale con Qwen in corso...")
    risultato = app.invoke(stato_iniziale)
    print("\n=== PROCESSO TERMINATO ===")
    print(risultato['messaggio'])