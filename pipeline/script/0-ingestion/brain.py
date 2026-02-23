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
def cerca_catalogo(query: str) -> str:
    """Usa questo tool ESCLUSIVAMENTE per cercare dati tecnici, valori numerici e specifiche di targa dei prodotti (es. portata, potenza, dimensioni, peso, voltaggio).
    QUANDO NON USARLO: Non usarlo per cercare istruzioni di montaggio, guide all'uso o informazioni sull'azienda.
    ISTRUZIONI PER LA QUERY: Inserisci nella query solo il nome della famiglia/modello e il parametro esatto richiesto dall'utente. Non inventare codici e non includere parti della domanda originale."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO")
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_catalogo:
        return "Errore: Database catalogo non caricato."
    
    docs = db_catalogo.similarity_search(query, k=5)
    
    if not docs:
        print("[TOOL] Nessun documento estratto.")
        return "Nessun dato trovato nel catalogo."
    
    risultati = []
    for d in docs:
        modello = d.metadata.get('modello_id', 'N/D')
        risultati.append(f"[Modello: {modello}] {d.page_content}")
    
    testo_finale = "\n".join(risultati)
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

@tool
def cerca_sito_web(query: str) -> str:
    """Usa questo tool per cercare procedure passo-passo, guide all'installazione, risoluzione dei problemi (troubleshooting), codici di errore, manutenzione e istruzioni di funzionamento.
    QUANDO NON USARLO: Non usarlo per cercare specifiche tecniche numeriche o presentazioni commerciali.
    ISTRUZIONI PER LA QUERY: Estrai le parole chiave relative all'azione o al problema (es. 'installazione valvola', 'errore E01', 'procedura pulizia filtro')."""
    print(f"\n[TOOL] Esecuzione CERCA_SITO_WEB")
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_web:
        return "Errore: Database sito non caricato."
        
    docs = db_web.similarity_search(query, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

@tool
def cerca_manuali(query: str) -> str:
    """Usa questo tool per trovare informazioni commerciali, descrizioni generali dell'azienda, contatti, policy, o panoramiche ad alto livello sui prodotti e servizi offerti.
    QUANDO NON USARLO: Non usarlo per risolvere guasti o per cercare dati tecnici specifici.
    ISTRUZIONI PER LA QUERY: Usa concetti generici come 'chi siamo', 'contatti', 'visione aziendale' o 'descrizione prodotto X'."""
    print(f"\n[TOOL] Esecuzione CERCA_MANUALI")
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_manuali:
        return "Errore: Database manuali non caricato."
        
    docs = db_manuali.similarity_search(query, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

tools = [cerca_catalogo, cerca_sito_web, cerca_manuali]

# configurazione LangGraph e LLM

llm = ChatOllama(model="llama3.1", temperature=0)
llm_with_tools = llm.bind_tools(tools)

def call_model(state: AgentState):
    print("\nL'intelligenza artificiale sta analizzando i dati e generando la risposta...")
    messages = state["messages"]
    response = llm_with_tools.invoke(messages)
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
    
    istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico preciso e diretto. 
    Quando ti vengono forniti dati da un catalogo, rispondi ESATTAMENTE alla domanda dell'utente. 
    Se l'utente chiede 'qual è il maggiore', confronta i dati estratti e scrivi solo il risultato vincente. 
    NON fare MAI lunghi elenchi riassuntivi a meno che l'utente non ti chieda esplicitamente 'elencami tutti i modelli'.""")
    
    while True:
        user_input = input("\nUtente: ")
        if user_input.lower() == 'esci':
            break
            
        initial_state = {"messages": [istruzioni_di_sistema, HumanMessage(content=user_input)]}
        result = app.invoke(initial_state)
        
        print(f"\nAssistente: {result['messages'][-1].content}")