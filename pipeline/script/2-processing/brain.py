import os
import sqlite3
import time
import pandas as pd
import difflib
import re
from typing import Annotated, List, TypedDict
import operator
from langgraph.graph import StateGraph, END
from langchain_ollama import ChatOllama
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma

class AgentState(TypedDict):
    messages: Annotated[List, operator.add]

#1 DEFINIZIONE FUNZIONI DI SUPPORTO

def get_collection_names(db_path: str) -> List[str]:
    # va a leggere fisicamente il file sqlite di chroma per estrarre le tabelle
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

def carica_database(nome_cartella_db: str, kb_name: str, embeddings) -> Chroma:
    # costruisce il percorso assoluto
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_dir = os.path.dirname(os.path.dirname(script_dir))

    percorso_assoluto = os.path.join(pipeline_dir, "data", "2-processing", nome_cartella_db)
    
    if not os.path.exists(percorso_assoluto):
        print(f"Errore: Il percorso {percorso_assoluto} non esiste.")
        return None
        
    collections = get_collection_names(percorso_assoluto)
    db_scelto = None
    
    for col in collections:
        db_temp = Chroma(
            persist_directory=percorso_assoluto,
            embedding_function=embeddings,
            collection_name=col
        )
        dati = db_temp.get()
        if len(dati['ids']) > 0:
            db_scelto = db_temp
            print(f"[{kb_name.upper()}] Collection '{col}' caricata con {len(dati['ids'])} documenti.")
            break
            
    return db_scelto

#2 DEFINIZIONE DEI TOOL

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
    # filtra la ricerca solo per i documenti che hanno questo esatto modello nei metadati
    docs = db_catalogo.similarity_search(parametro_richiesto, k=5, filter={"modello_id": codice_pulito})
    
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

    # rimuove i finti trattini bassi che l'llm inventa spesso
    richiesta_esatta = parametro_richiesto.replace('_', ' ').strip().lower()
    colonna_reale = None
    
    for col in colonne_catalogo:
        col_pulita_spazi = " ".join(col.split()).lower()
        richiesta_pulita_spazi = " ".join(richiesta_esatta.split())
        if col_pulita_spazi == richiesta_pulita_spazi:
            colonna_reale = col
            break

    # piano b se la colonna non matcha esattamente
    if not colonna_reale:
        richiesta_pulita = re.sub(r'[^a-zA-Z0-9]', ' ', parametro_richiesto).lower()
        parole_richiesta = []
        for p in richiesta_pulita.split():
            if len(p) > 0:
                parole_richiesta.append(p)
        
        # assegna un punteggio ad ogni colonna in base a quante parole combaciano
        punteggi = {}
        for col in colonne_catalogo:
            col_pulita = re.sub(r'[^a-zA-Z0-9]', ' ', col).lower()
            parole_col = col_pulita.split()
            
            score = 0
            for pr in parole_richiesta:
                for pc in parole_col:
                    if pr == pc or pr in pc:
                        score = score + 1
            
            if score > 0:
                punteggi[col] = score

        if len(punteggi) == 0:
            match_simili = difflib.get_close_matches(parametro_richiesto.replace('_', ' '), list(colonne_catalogo), n=5, cutoff=0.1)
            
            if len(match_simili) > 0:
                opzioni = match_simili
            else:
                opzioni = colonne_catalogo[:5]
                
            testo_opzioni = ""
            for opt in opzioni:
                testo_opzioni = testo_opzioni + "- " + opt + "\n"
                
            return "Dì all'utente ESATTAMENTE questo: 'Per favore, copia e incolla ESATTAMENTE una di queste opzioni nella chat:\n" + testo_opzioni + "'"

        max_score = 0
        for val in punteggi.values():
            if val > max_score:
                max_score = val
                
        soglia = len(parole_richiesta) * 0.5
        if soglia < 1:
            soglia = 1
        
        migliori_colonne = []
        for col, score in punteggi.items():
            if score == max_score and score >= soglia:
                migliori_colonne.append(col)

        # se c'e un pareggio chiede aiuto all'utente
        if len(migliori_colonne) > 1:
            testo_opzioni = ""
            for opt in migliori_colonne:
                testo_opzioni = testo_opzioni + "- " + opt + "\n"
            return "Dì all'utente ESATTAMENTE questo: 'Il parametro è ambiguo. Per favore, copia e incolla ESATTAMENTE una di queste opzioni nella chat:\n" + testo_opzioni + "'"
            
        elif len(migliori_colonne) == 1:
            colonna_reale = migliori_colonne[0]
            print(f"[TOOL] Match parziale trovato: '{parametro_richiesto}' diventerà '{colonna_reale}'")
        else:
            return "Nessuna colonna valida trovata. Chiedi all'utente di riformulare la domanda."
    else:
        print(f"[TOOL] Match ESATTO trovato per '{colonna_reale}'")

    df = df_catalogo.copy()
    
    # fix artigianale per convertire i numeri col punto stile italiano
    def pulisci_numero(valore):
        if isinstance(valore, str):
            valore_senza_punti = valore.replace('.', '')
            valore_con_punto_decimale = valore_senza_punti.replace(',', '.')
            return valore_con_punto_decimale
        else:
            return valore
            
    df[colonna_reale] = df[colonna_reale].apply(pulisci_numero)
    df[colonna_reale] = pd.to_numeric(df[colonna_reale], errors="coerce")
    
    # capisce come fare la classifica in base al parametro dell'llm
    ordinamento_minuscolo = ordinamento.strip().lower()
    if ordinamento_minuscolo == "crescente":
        deve_crescere = True
    else:
        deve_crescere = False
        
    risultato = df.dropna(subset=[colonna_reale])
    risultato = risultato.sort_values(by=colonna_reale, ascending=deve_crescere)
    risultato = risultato.head(top_n)

    # crea l'elenco testuale per evitare che l'llm si confonda leggendo una tabella
    testo_finale = ""
    for index, row in risultato.iterrows():
        nome_modello = row.get("Modello PAL", "Sconosciuto")
        valore = row.get(colonna_reale, "N/D")
        testo_finale = testo_finale + f"- Modello: {nome_modello} | Valore: {valore}\n"

    return testo_finale


@tool
def cerca_sito_web(query: str) -> str:
    """Usa questo tool per cercare procedure passo-passo, guide all'installazione, troubleshooting e codici di errore.
    REGOLA FERREA: Usa un UNICO parametro stringa chiamato 'query' contenente le parole chiave."""
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_web:
        return "Errore: Database sito non caricato."
        
    docs = db_web.similarity_search(query, k=5)
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
        
    docs = db_manuali.similarity_search(query, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

#3 FUNZIONI DI LANGGRAPH

def call_model(state: AgentState):
    print("\nL'intelligenza artificiale sta analizzando i dati e generating la risposta...")
    messages = state["messages"]
    response = llm_with_tools.invoke(messages)
    
    # printa a schermo le intenzioni dell'ai per capire cosa sta combinando
    if response.tool_calls:
        tool_name = response.tool_calls[0].get('name', 'Sconosciuto')
        tool_args = response.tool_calls[0].get('args', {})
        print(f"[DEBUG LLM] Tentativo di chiamata al tool '{tool_name}' con argomenti: {tool_args}")
    
    return {"messages": [response]}

def should_continue(state: AgentState) -> str:
    # controlla se l'ai ha deciso di usare un tool o se ha finito
    last_message = state["messages"][-1]
    if last_message.tool_calls:
        return "tools"
    return "end"


#4 INIZIALIZZAZIONE E SETUP

print("Inizializzazione sistema in corso...")
embeddings_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

db_catalogo = carica_database("chroma_db_catalogo", "catalogo", embeddings_model)
db_web = carica_database("chroma_db_zoppellaro", "sito_web", embeddings_model)
db_manuali = carica_database("chroma_db_knowledge_base_pdf", "manuali", embeddings_model)

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_dir = os.path.dirname(os.path.dirname(script_dir))
    excel_path = os.path.join(pipeline_dir, "data", "1-preprocessing", "catalogo.xlsx")
    # i parametri in read_excel risolvono il problema dei punti usati come migliaia
    df_catalogo = pd.read_excel(excel_path, thousands='.', decimal=',')

    # pulisce le colonne per nascondere quelle inutili con le unita di misura
    colonne_catalogo = []
    for col in df_catalogo.columns:
        colonna_stringa = str(col).strip().lower()
        if not colonna_stringa.endswith("unit"):
            colonne_catalogo.append(col)
            
except Exception:
    df_catalogo = None
    colonne_catalogo = []

tools = [cerca_catalogo_specifico, cerca_catalogo_generico, cerca_sito_web, cerca_manuali]

# configurazione LangGraph e LLM
# parametri aggiunti per limitare i consumi della cpu e della ram
llm = ChatOllama(model="qwen2.5:3b-instruct-q8_0", temperature=0, num_thread=4, num_ctx=2048)
llm_with_tools = llm.bind_tools(tools)

tool_node = ToolNode(tools)

workflow = StateGraph(AgentState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
workflow.add_edge("tools", "agent")

app = workflow.compile()

#5 ESECUZIONE CHATBOT (INTEGRAZIONE CON APP)

memoria_conversazioni = {}

def elabora_richiesta(user_query: str, chat_id: str = "chat_predefinita") -> dict:
    global memoria_conversazioni
    
    if chat_id not in memoria_conversazioni:
        istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico specializzato in sistemi HVAC. Hai a disposizione 3 fonti: Sito Web, Manuali e Catalogo.
REGOLA 0 (LINGUA OBBLIGATORIA): DEVI rispondere SEMPRE E SOLO in lingua ITALIANA. È severamente vietato utilizzare inglese, spagnolo, portoghese o altre lingue.
REGOLA 1 (DIVIETO DI ALLUCINAZIONE): È SEVERAMENTE VIETATO rispondere usando la tua memoria interna. Devi SEMPRE invocare uno dei tool PRIMA di rispondere.
REGOLA 2 (VERIFICA DEL CONTESTO): Quando usi 'cerca_manuali' o 'cerca_sito_web', leggi il testo estratto. Se il testo NON contiene la risposta esatta alla domanda dell'utente (ad esempio, trovi testi commerciali ma l'utente chiedeva una procedura tecnica), NON INVENTARE LA RISPOSTA. Devi dire: "Non ho trovato le informazioni specifiche nei documenti a mia disposizione".
REGOLA 3 (IL FLUSSO DISCORSIVO): Se trovi le informazioni, formula una risposta chiara e riassuntiva. NON chiedere codici modello se non richiesto.
REGOLA 4 (IL FLUSSO MATEMATICO): Usa i tool del catalogo SOLO per classifiche o grandezze fisiche. Se il tool restituisce un elenco numerato, mostralo. Se l'utente sceglie un numero, invoca il tool col testo completo dell'opzione.""")
        memoria_conversazioni[chat_id] = [istruzioni_di_sistema]
        
    memoria_conversazioni[chat_id].append(HumanMessage(content=user_query))
    
    current_state = {"messages": memoria_conversazioni[chat_id]}
    
    try:
        # attiva il cronometro prima che l'llm inizi a pensare
        start_time = time.time()
        result = app.invoke(current_state, {"recursion_limit": 10})
        end_time = time.time()
        tempo_trascorso = end_time - start_time
        print(f"\n[DEBUG TEMPO] Tempo di risposta: {tempo_trascorso:.2f} secondi")
    except Exception as e:
        return {"testo": f"Si è verificato un errore nel motore: {e}", "azioni": []}
        
    memoria_conversazioni[chat_id] = result['messages']
    risposta_assistente = result['messages'][-1].content
    
    # estrae i nomi dei tool usati per mostrarli nell'interfaccia grafica
    tool_usati = []
    for msg in result['messages']:
        if hasattr(msg, 'tool_calls') and msg.tool_calls:
            for tool in msg.tool_calls:
                if tool['name'] not in tool_usati:
                    tool_usati.append(tool['name'])
    #aggiunge alla memoria SOLO la risposta finale, senza il ragionamento dietro
    memoria_conversazioni[chat_id].append(AIMessage(content=risposta_assistente))
                    
    return {
        "testo": risposta_assistente,
        "azioni": tool_usati
    }