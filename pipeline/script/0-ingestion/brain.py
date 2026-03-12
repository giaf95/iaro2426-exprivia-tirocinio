import os
import sqlite3
import tkinter as tk
import pandas as pd
from tkinter import filedialog
from typing import Annotated, List, TypedDict
import operator
from langgraph.graph import StateGraph, END
from langchain_ollama import ChatOllama
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma

class AgentState(TypedDict):
    messages: Annotated[List, operator.add]

def get_collection_names(db_path: str) -> List[str]:
    sqlite_path = os.path.join(db_path, "chroma.sqlite3")
    if not os.path.exists(sqlite_path):
        return []
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM collections")
        collections = [row[0] for row in cursor.fetchall()]
        conn.close()
        return collections
    except Exception:
        return []

def select_and_load_db(kb_name: str, embeddings) -> Chroma:
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    path = filedialog.askdirectory(title=f"DB {kb_name.upper()}")
    root.destroy()
    
    if not path:
        return None
        
    collections = get_collection_names(path)
    db_scelto = None
    
    for col in collections:
        db_temp = Chroma(
            persist_directory=path,
            embedding_function=embeddings,
            collection_name=col
        )
        dati = db_temp.get()
        if len(dati['ids']) > 0:
            db_scelto = db_temp
            print(f"[{kb_name.upper()}] Collection '{col}' caricata con {len(dati['ids'])} documenti.")
            break
            
    return db_scelto

print("Inizializzazione sistema in corso...")
embeddings_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

db_catalogo = select_and_load_db("catalogo", embeddings_model)
db_web = select_and_load_db("sito_web", embeddings_model)
db_manuali = select_and_load_db("manuali", embeddings_model)

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_dir = os.path.dirname(os.path.dirname(script_dir))
    excel_path = os.path.join(pipeline_dir, "data", "1-preprocessing", "catalogo.xlsx")
    df_catalogo = pd.read_excel(excel_path)
    colonne_catalogo = list(df_catalogo.columns)
except Exception:
    df_catalogo = None
    colonne_catalogo = []


# definizione dei tool

@tool
def cerca_catalogo_specifico(codice_modello: str, parametro_richiesto: str) -> str:
    """Usa questo tool ESCLUSIVAMENTE quando l'utente chiede un dato tecnico di un MODELLO SPECIFICO (es. 'portata del modello 061-035').
    QUANDO NON USARLO: Non usarlo per classifiche, confronti tra molti modelli o per cercare campi di applicazione (es. sale operatorie).
    ISTRUZIONI: 
    1. 'codice_modello': estrai SOLO il codice (es. '061-035').
    2. 'parametro_richiesto': la grandezza fisica da cercare."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO_SPECIFICO")
    print(f"[TOOL] Ricerca chirurgica -> Modello: '{codice_modello}' | Parametro: '{parametro_richiesto}'")
    
    if not db_catalogo:
        return "Errore: Database catalogo non caricato."
    
    codice_pulito = codice_modello.lower().replace("modello", "").strip()
    docs = db_catalogo.similarity_search(parametro_richiesto, k=3, filter={"modello_id": codice_pulito})
    
    if not docs:
        return "Nessun dato trovato nel catalogo per questo modello specifico."
    
    risultati = [f"[Modello: {d.metadata.get('modello_id', 'N/D')}] {d.page_content}" for d in docs]
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return "\n".join(risultati)

@tool
def cerca_catalogo_generico(parametro_richiesto: str, top_n: int = 3) -> str:
    """Usa questo tool ESCLUSIVAMENTE per domande analitiche e matematiche sul catalogo, come trovare classifiche, i valori massimi, minimi o "i top 3".
    QUANDO NON USARLO: Non usarlo per cercare testo o descrizioni.
    PARAMETRI:
    - 'parametro_richiesto': il NOME ESATTO della colonna così come appare nel file Excel del catalogo. Non usare snake_case, non tradurre, non abbreviare.
    - 'top_n': il numero di modelli da restituire."""
    print(f"\n[TOOL] Esecuzione cerca_catalogo_generico")
    print(f"[TOOL] Estrazione dati strutturati -> Parametro: '{parametro_richiesto}', Top: {top_n}")

    if df_catalogo is None:
        return "Errore: file Excel del catalogo non caricato."

    richiesta_norm = parametro_richiesto.strip().lower()
    colonne_norm = {col.strip().lower(): col for col in colonne_catalogo}

    if richiesta_norm not in colonne_norm:
        return (
            "Il parametro richiesto non corrisponde a nessuna colonna del catalogo.\n"
            "DEVI scegliere esattamente uno dei seguenti nomi di colonna (copiali e incollali senza modifiche):\n"
            f"{', '.join(colonne_catalogo)}"
        )

    colonna_reale = colonne_norm[richiesta_norm]

    df = df_catalogo.copy()
    df[colonna_reale] = pd.to_numeric(df[colonna_reale], errors="coerce")
    risultato = df.dropna(subset=[colonna_reale]).sort_values(by=colonna_reale, ascending=False).head(top_n)

    colonne_output = ["Modello PAL", colonna_reale]
    colonne_esistenti = [col for col in colonne_output if col in df.columns]

    return risultato[colonne_esistenti].to_string(index=False)


@tool
def cerca_sito_web(query: str) -> str:
    """Usa questo tool per cercare procedure passo-passo, guide all'installazione, troubleshooting e codici di errore.
    REGOLA FERREA: Usa un UNICO parametro stringa chiamato 'query' contenente le parole chiave."""
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_web:
        return "Errore: Database sito non caricato."
        
    docs = db_web.similarity_search(query, k=2)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

@tool
def cerca_manuali(query: str) -> str:
    """Usa questo tool per trovare informazioni commerciali, contatti, o AMBIENTI DI APPLICAZIONE (es. sale operatorie, ospedali, uso industriale).
    QUANDO NON USARLO: Non usarlo per cercare dati tecnici numerici o modelli.
    REGOLA FERREA: Usa un UNICO parametro stringa chiamato 'query'."""
    print(f"\n[TOOL] Esecuzione CERCA_MANUALI")
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_manuali:
        return "Errore: Database manuali non caricato."
        
    docs = db_manuali.similarity_search(query, k=2)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

tools = [cerca_catalogo_specifico, cerca_catalogo_generico, cerca_sito_web, cerca_manuali]

# configurazione LangGraph e LLM

llm = ChatOllama(model="qwen2.5:3b", temperature=0)
llm_with_tools = llm.bind_tools(tools)

def call_model(state: AgentState):
    print("\nL'intelligenza artificiale sta analizzando i dati e generando la risposta...")
    messages = state["messages"]
    response = llm_with_tools.invoke(messages)
    
    # debug
    if response.tool_calls:
        tool_name = response.tool_calls[0].get('name', 'Sconosciuto')
        tool_args = response.tool_calls[0].get('args', {})
        print(f"[DEBUG LLM] Tentativo di chiamata al tool '{tool_name}' con argomenti: {tool_args}")
    
    return {"messages": [response]}

tool_node = ToolNode(tools)

def should_continue(state: AgentState) -> str:
    last_message = state["messages"][-1]
    if last_message.tool_calls:
        return "tools"
    return "end"

workflow = StateGraph(AgentState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
workflow.add_edge("tools", "agent")

app = workflow.compile()

# esecuzione chatbot

if __name__ == "__main__":
    print("\nChatbot Tool-Based avviato. Scrivi 'esci' per terminare.")
    
    istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico di prevendita preciso e analitico.
REGOLA 1: Usa sempre gli strumenti a tua disposizione prima di rispondere.
REGOLA 2 (ANTI-ALLUCINAZIONE): Rispondi ESCLUSIVAMENTE basandoti sul testo estratto dai tool. Non generare testo basato sulle tue conoscenze interne. Se le informazioni fornite dai tool contengono errori o dicono "non trovato", rispondi all'utente che non hai a disposizione quei dati nel catalogo.
REGOLA 3: Per classifiche e valori massimi, usa sempre 'cerca_catalogo_generico' e riporta i numeri esatti che ti restituisce.
REGOLA 4 (PARAMETRO CATALOGO): Quando chiami 'cerca_catalogo_generico', il campo 'parametro_richiesto' DEVE essere esattamente uguale al nome di una colonna del file Excel del catalogo, senza snake_case, senza traduzioni e senza abbreviazioni.
SE RICEVI dal tool una risposta che elenca le colonne disponibili e ti dice di scegliere un nome esatto, SCEGLI TU la colonna più adatta (quella che contiene il dato richiesto dall'utente) e fai una NUOVA CHIAMATA al tool usando quel nome copiato letteralmente. NON CHIEDERE CONFERMA all'utente, agisci autonomamente.
SE RICEVI dal tool una risposta che elenca le colonne disponibili e ti dice di scegliere un nome esatto, DEVI fare una nuova chiamata al tool usando una di quelle colonne copiata letteralmente, senza modifiche. Se non trovi nessuna colonna adatta, devi dirlo all'utente e NON inventare dati o parametri.""")

    
    while True:
        user_input = input("\nUtente: ")
        if user_input.lower() == 'esci':
            break
            
        initial_state = {"messages": [istruzioni_di_sistema, HumanMessage(content=user_input)]}
        # aggiunto limite di ricorsione per bloccare i loop infiniti
        result = app.invoke(initial_state, {"recursion_limit": 10})
        
        print(f"\nAssistente: {result['messages'][-1].content}")