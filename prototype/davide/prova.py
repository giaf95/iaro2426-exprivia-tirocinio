import os
import sqlite3
from typing import Annotated, List, TypedDict, Union
import operator
from langgraph.graph import StateGraph, END
from langchain_ollama import ChatOllama
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma

# =================================================================
# CONFIGURAZIONE PERCORSI (Sostituisci con i tuoi percorsi reali)
# =================================================================
PERCORSI_DB = {
    "catalogo": r"C:/Users/PC_A87/Desktop/Carricamento Progetti GIT/pipeline/data/2-processing/chroma_db_catalogo",
    "sito": r"C:/Users/PC_A87/Desktop/Carricamento Progetti GIT/pipeline/data/2-processing/chroma_db_zoppellaro",
    "pdf": r"C:/Users/PC_A87/Desktop/Carricamento Progetti GIT/pipeline/data/2-processing/chroma_db_knowledge_base_pdf"
}

# Modello di Embedding (condiviso)
embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

# =================================================================
# DEFINIZIONE TOOL (Logica RAG)
# =================================================================

def get_collection_names(db_path: str) -> List[str]:
    sqlite_path = os.path.join(db_path, "chroma.sqlite3")
    if not os.path.exists(sqlite_path): return []
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM collections")
        collections = [row[0] for row in cursor.fetchall()]
        conn.close()
        return collections
    except: return []

def search_in_db(db_path: str, query: str, top_k: int = 5):
    if not os.path.exists(db_path): 
        return "ERRORE: Database assente per questo tool. Dì all'utente che non hai i dati e NON INVENTARE NESSUNA INFORMAZIONE."
    
    collections = get_collection_names(db_path)
    if not collections: return "Nessun dato trovato."
    
    db = Chroma(persist_directory=db_path, embedding_function=embeddings, collection_name=collections[0])
    results = db.similarity_search(query, k=top_k)
    return "\n".join([res.page_content for res in results])

@tool
def cerca_catalogo(query: str):
    """USA QUESTO TOOL SOLO ED ESCLUSIVAMENTE per cercare specifiche tecniche numeriche, codici di prodotto e dimensioni dei modelli."""
    return search_in_db(PERCORSI_DB["catalogo"], query)

@tool
def cerca_sito_web(query: str):
    """USA QUESTO TOOL per trovare la storia dell'azienda, 'chi siamo', email, numeri di telefono o contatti generali."""
    return search_in_db(PERCORSI_DB["sito"], query)

@tool
def cerca_manuali_pdf(query: str):
    """USA QUESTO TOOL per trovare procedure, guide all'installazione, risoluzione di errori (troubleshooting) e manutenzione."""
    return search_in_db(PERCORSI_DB["pdf"], query)

tools = [cerca_catalogo, cerca_sito_web, cerca_manuali_pdf]
tool_node = ToolNode(tools)

# =================================================================
# LOGICA AGENTE (LangGraph)
# =================================================================

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]

model = ChatOllama(model="llama3.1", temperature=0).bind_tools(tools)

def should_continue(state: AgentState):
    last_message = state["messages"][-1]
    return "tools" if last_message.tool_calls else "end"

def call_model(state: AgentState):
    return {"messages": [model.invoke(state["messages"])]}

# Costruzione del Grafo
workflow = StateGraph(AgentState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)
workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
workflow.add_edge("tools", "agent")

# Compilazione dell'App
app_engine = workflow.compile()

# =================================================================
# IL "CONTRATTO" - FUNZIONE PER L'INTERFACCIA
# =================================================================

def elabora_richiesta(user_input: str):
    """
    Questa funzione è il ponte tra il motore e l'interfaccia.
    Restituisce un dizionario standardizzato.
    """
    istruzioni_sistema = SystemMessage(content="""Sei l'assistente tecnico ufficiale dell'azienda Zoppellaro. 
    REGOLE FERREE E OBBLIGATORIE:
    1. Usa il tool più appropriato analizzando la domanda dell'utente.
    2. Basa la tua risposta ESCLUSIVAMENTE sui dati restituiti dai tool.
    3. Se il tool ti dice 'Nessun dato trovato' o 'Database assente', DEVI RISPONDERE testualmente: 'Mi dispiace, ma non trovo questa informazione nei miei documenti aziendali'. 
    4. È SEVERAMENTE VIETATO inventare informazioni, numeri o procedure usando le tue conoscenze generali.""")
    
    # Esecuzione del grafo
    inputs = {"messages": [istruzioni_sistema, HumanMessage(content=user_input)]}
    config = {"configurable": {"thread_id": "session_1"}}
    result = app_engine.invoke(inputs, config)
    
    # Estrazione della risposta testuale
    risposta_testo = result["messages"][-1].content
    
    # Estrazione delle azioni compiute (nomi dei tool chiamati)
    azioni = []
    for m in result["messages"]:
        if hasattr(m, 'tool_calls') and m.tool_calls:
            for call in m.tool_calls:
                azioni.append(call['name'])
    
    # Restituzione del formato standardizzato (Il Contratto)
    return {
        "testo": risposta_testo,
        "azioni": list(set(azioni)),  # Lista tool unici
        "dati": {}                    # Qui potresti estrarre tabelle se necessario
    }