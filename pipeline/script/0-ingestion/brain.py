import os
import sqlite3
import tkinter as tk
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
def cerca_catalogo_generico(parametro_richiesto: str) -> str:
    """Usa questo tool ESCLUSIVAMENTE per domande generiche sul catalogo, confronti, o per trovare 'i migliori', 'i maggiori', 'i primi 3' (es. 'quali modelli hanno la portata più alta?').
    QUANDO NON USARLO: Non usarlo se l'utente ha fornito un codice modello specifico o se cerca campi di applicazione.
    ISTRUZIONI: Usa un UNICO parametro stringa con la grandezza fisica da cercare (es. 'Portata Massima Ripresa')."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO_GENERICO")
    print(f"[TOOL] Ricerca a strascico -> Parametro: '{parametro_richiesto}'")
    
    if not db_catalogo:
        return "Errore: Database catalogo non caricato."
    
    # K molto alto per pescare quanti più modelli possibili da far confrontare all'LLM
    docs = db_catalogo.similarity_search(parametro_richiesto, k=25)
    
    risultati = [f"[Modello: {d.metadata.get('modello_id', 'N/D')}] {d.page_content}" for d in docs]
    print(f"[TOOL] Estratti {len(docs)} documenti da far confrontare all'LLM.")
    return "\n".join(risultati)

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

llm = ChatOllama(model="llama3.1", temperature=0)
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
    
    istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico preciso e analitico. 
    REGOLA 1 (ANTI-ALLUCINAZIONE): Rispondi ESCLUSIVAMENTE basandoti sul testo estratto dai tool. Se l'informazione (come un numero verde) non c'è, scrivi: "L'informazione non è presente nei documenti forniti."
    REGOLA 2 (CLASSIFICHE): Se l'utente chiede i "top 3" o il "maggiore", DEVI estrarre mentalmente tutti i valori numerici dai documenti ricevuti, ordinarli matematicamente in modo decrescente e poi rispondere elencando i modelli corretti. Non fermarti al primo che leggi.""")
    
    while True:
        user_input = input("\nUtente: ")
        if user_input.lower() == 'esci':
            break
            
        initial_state = {"messages": [istruzioni_di_sistema, HumanMessage(content=user_input)]}
        # aggiunto limite di ricorsione per bloccare i loop infiniti
        result = app.invoke(initial_state, {"recursion_limit": 10})
        
        print(f"\nAssistente: {result['messages'][-1].content}")