import os
import sqlite3
import tkinter as tk
import pandas as pd
import difflib
import re
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
    colonne_catalogo = [col for col in df_catalogo.columns if not str(col).strip().lower().endswith("unit")]
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
def cerca_catalogo_generico(parametro_richiesto: str, ordinamento: str = "decrescente", top_n: int = 3) -> str:
    """Usa questo tool ESCLUSIVAMENTE per domande analitiche e matematiche sul catalogo.
    REGOLA FONDAMENTALE: Usa SOLO i parametri 'parametro_richiesto', 'ordinamento' e 'top_n'. È severamente vietato inventare altri parametri.
    PARAMETRI:
    - 'parametro_richiesto': Inserisci ESATTAMENTE il nome della colonna incollato dall'utente.
    - 'ordinamento': inserisci la parola 'crescente' se l'utente cerca i valori più bassi o minimi. Inserisci 'decrescente' se cerca i più alti o massimi.
    - 'top_n': il numero di modelli da restituire."""
    print(f"\n[TOOL] Esecuzione cerca_catalogo_generico")
    print(f"[TOOL] Estrazione -> Parametro: '{parametro_richiesto}', Ordine: '{ordinamento}', Top: {top_n}")

    if df_catalogo is None:
        return "Errore: file Excel non caricato."

    richiesta_esatta = parametro_richiesto.replace('_', ' ').strip().lower()
    colonna_reale = None
    
    for col in colonne_catalogo:
        if " ".join(col.split()).lower() == " ".join(richiesta_esatta.split()):
            colonna_reale = col
            break

    if not colonna_reale:
        richiesta_pulita = re.sub(r'[^a-zA-Z0-9]', ' ', parametro_richiesto).lower()
        parole_richiesta = [p for p in richiesta_pulita.split() if len(p) > 0]
        
        punteggi = {}
        for col in colonne_catalogo:
            col_pulita = re.sub(r'[^a-zA-Z0-9]', ' ', col).lower()
            parole_col = col_pulita.split()
            
            score = 0
            for pr in parole_richiesta:
                if any(pr == pc or pr in pc for pc in parole_col):
                    score += 1
            if score > 0:
                punteggi[col] = score

        if not punteggi:
            match_simili = difflib.get_close_matches(parametro_richiesto.replace('_', ' '), list(colonne_catalogo), n=5, cutoff=0.1)
            opzioni = match_simili if match_simili else colonne_catalogo[:5]
            lista_opzioni = "\n".join([f"- {opt}" for opt in opzioni])
            return f"Dì all'utente ESATTAMENTE questo: 'Per favore, copia e incolla ESATTAMENTE una di queste opzioni nella chat:\n{lista_opzioni}'"

        max_score = max(punteggi.values())
        soglia = max(1, len(parole_richiesta) * 0.5)
        
        migliori_colonne = [col for col, score in punteggi.items() if score == max_score and score >= soglia]
        
        if len(migliori_colonne) > 1:
            lista_opzioni = "\n".join([f"- {opt}" for opt in migliori_colonne])
            return f"Dì all'utente ESATTAMENTE questo: 'Il parametro è ambiguo. Per favore, copia e incolla ESATTAMENTE una di queste opzioni nella chat:\n{lista_opzioni}'"
        elif len(migliori_colonne) == 1:
            colonna_reale = migliori_colonne[0]
            print(f"[TOOL] Match parziale: '{parametro_richiesto}' -> '{colonna_reale}'")
        else:
            return "Nessuna colonna valida trovata. Chiedi all'utente di riformulare."
    else:
        print(f"[TOOL] Match ESATTO: '{parametro_richiesto}' -> '{colonna_reale}'")

    df = df_catalogo.copy()
    df[colonna_reale] = df[colonna_reale].apply(lambda x: str(x).replace('.', '').replace(',', '.') if isinstance(x, str) else x)
    df[colonna_reale] = pd.to_numeric(df[colonna_reale], errors="coerce")
    
    is_ascending = True if ordinamento.strip().lower() == "crescente" else False
    risultato = df.dropna(subset=[colonna_reale]).sort_values(by=colonna_reale, ascending=is_ascending).head(top_n)

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
    
    istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico preciso e analitico.
REGOLA 1: Usa sempre gli strumenti a tua disposizione prima di rispondere.
REGOLA 2: Rispondi ESCLUSIVAMENTE basandoti sul testo estratto dai tool.
REGOLA 3: Per classifiche o valori massimi/minimi, usa sempre 'cerca_catalogo_generico'.
REGOLA 4 (IL COPIA E INCOLLA): Se il tool ti dice di far copiare e incollare un'opzione, riporta il messaggio all'utente.
REGOLA 5 (LA TUA REAZIONE): Quando l'utente incolla l'opzione, DEVI chiamare immediatamente 'cerca_catalogo_generico' inserendo TUTTO il testo dell'utente dentro 'parametro_richiesto'. NON inserire la parola 'modello' e NON inventare parametri aggiuntivi.""")
    cronologia_messaggi = [istruzioni_di_sistema]
    
    while True:
        user_input = input("\nUtente: ")
        if user_input.lower() == 'esci':
            break
            
        cronologia_messaggi.append(HumanMessage(content=user_input))
        
        current_state = {"messages": cronologia_messaggi}
        result = app.invoke(current_state, {"recursion_limit": 10})
        
        risposta_assistente = result['messages'][-1]
        print(f"\nAssistente: {risposta_assistente.content}")
        
        cronologia_messaggi = result['messages']