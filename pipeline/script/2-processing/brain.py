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
    """Usa questo tool ESCLUSIVAMENTE quando l'utente fornisce un CODICE ALFANUMERICO ESATTO di un modello (es. '061-035') e vuole sapere un suo dato tecnico.
    ISTRUZIONI: 
    1. 'codice_modello': estrai SOLO il codice esatto (es. '061-035').
    2. 'parametro_richiesto': la grandezza fisica da cercare."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO_SPECIFICO")
    print(f"[TOOL] Ricerca chirurgica -> Modello: '{codice_modello}' | Parametro: '{parametro_richiesto}'")
    
    if df_catalogo is None:
        return "Errore: file Excel non caricato."
    
    # pulizia del codice cercato
    codice_pulito = codice_modello.upper().replace("MODELLO", "").strip()
    
    # cerca la riga esatta nel DataFrame Pandas
    df_modello = df_catalogo[df_catalogo['Modello PAL'].astype(str).str.upper().str.contains(codice_pulito, na=False)]
    
    if df_modello.empty:
        return f"Modello {codice_pulito} non trovato nel catalogo Excel."
        
    # cerca le colonne che contengono la parola richiesta
    richiesta_pulita = parametro_richiesto.lower().strip()
    colonne_trovate = [col for col in colonne_catalogo if richiesta_pulita in str(col).lower()]
    
    if not colonne_trovate:
         return f"Il parametro '{parametro_richiesto}' non esiste nel catalogo. Dì all'utente di specificare meglio la parola chiave."
         
    # estrae tutti i parametri trovati
    risultati = []
    for col in colonne_trovate:
        valore = df_modello.iloc[0].get(col, "N/D")
        risultati.append(f"- {col}: {valore}")
        
    return f"Dati tecnici per il modello {codice_pulito}:\n" + "\n".join(risultati)

@tool
def cerca_catalogo_generico(parametro_richiesto: str, ordinamento: str = "decrescente", top_n: int = 3, valore_target: float = None) -> str:
    """Usa questo tool ESCLUSIVAMENTE per domande analitiche e matematiche sul catalogo.
    REGOLA FONDAMENTALE: Usa SOLO i parametri richiesti.
    PARAMETRI:
    - 'parametro_richiesto': Inserisci ESATTAMENTE il nome della colonna.
    - 'ordinamento': 'crescente' o 'decrescente'.
    - 'top_n': il numero di modelli da restituire.
    - 'valore_target': (OPZIONALE) Se l'utente o il Calcolatore ti chiedono un modello per coprire un certo fabbisogno in kW, inserisci qui il numero. Il tool filtrerà i modelli adatti."""
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
            if valore_target is not None:
                # Se c'è un target, siamo in un flusso automatico: forza la prima opzione
                colonna_reale = migliori_colonne[0]
            else:
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
    
    risultato = df.dropna(subset=[colonna_reale])

    # ricerca a target
    if valore_target is not None:
        # tieni solo i modelli con potenza uguale o superiore al target richiesto
        risultato = risultato[risultato[colonna_reale] >= valore_target]
        # ordina in modo crescente per dare il modello appena sufficiente
        risultato = risultato.sort_values(by=colonna_reale, ascending=True)
    else:
        # vecchia logica per i massimi e minimi assoluti
        ordinamento_minuscolo = ordinamento.strip().lower()
        if ordinamento_minuscolo == "crescente":
            deve_crescere = True
        else:
            deve_crescere = False
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

@tool
def calcola_fabbisogno_termico(area_mq: float, numero_persone: int, delta_t: float, tipo_locale: str) -> str:
    """Usa questo tool per calcolare i kW (potenza frigorifera/termica) necessari per condizionare una stanza.
    Se l'utente non fornisce questi dati, CHIEDILI prima di usare il tool.
    PARAMETRI:
    - area_mq: metri quadri della stanza (es. 30).
    - numero_persone: quante persone occupano la stanza (es. 50).
    - delta_t: differenza di temperatura tra esterno e interno in gradi (es. fuori 35, dentro 20 = delta_t di 15).
    - tipo_locale: es. 'discoteca', 'ufficio', 'residenziale', 'palestra'."""
    print(f"\n[TOOL] Esecuzione CALCOLA_FABBISOGNO_TERMICO")
    print(f"[TOOL] Dati: {area_mq}mq, {numero_persone} persone, dT {delta_t}°, locale: {tipo_locale}")

    # 1. Carico Base Strutturale (W/mq)
    w_mq = 100 # Default per residenziale/uffici
    tipo_locale_low = tipo_locale.lower()
    if "discoteca" in tipo_locale_low or "palestra" in tipo_locale_low or "industria" in tipo_locale_low:
        w_mq = 150

    carico_base = area_mq * w_mq

    # 2. Carico Persone (W/persona) - a riposo emettono meno, in discoteca/palestra emettono molto calore
    w_persona = 100
    if "discoteca" in tipo_locale_low or "palestra" in tipo_locale_low:
        w_persona = 250

    carico_persone = numero_persone * w_persona

    # 3. Moltiplicatore Delta T (aumenta del 5% per ogni grado di sbalzo termico oltre i 10°C)
    moltiplicatore_delta = 1.0
    if delta_t > 10:
        gradi_extra = delta_t - 10
        moltiplicatore_delta = 1.0 + (gradi_extra * 0.05)

    # Calcolo Finale
    fabbisogno_totale_watt = (carico_base + carico_persone) * moltiplicatore_delta
    fabbisogno_kw = fabbisogno_totale_watt / 1000

    return f"Calcolo completato. Il fabbisogno termico stimato per questo {tipo_locale} è di {fabbisogno_kw:.2f} kW. INSTRUZIONE PER L'AI: Ora usa il tool 'cerca_catalogo_generico' per cercare i modelli con una 'Potenza Frigorifera' (o parametro simile) uguale o leggermente superiore a {fabbisogno_kw:.2f} kW."

#3 FUNZIONI DI LANGGRAPH E LOGICA AI

def call_model(state: AgentState):
    print("\nL'intelligenza artificiale sta analizzando i dati e generating la risposta...")
    messages = state["messages"]
    response = llm_con_tools.invoke(messages)
    
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

#4 FUNZIONI DI INTERFACCIA (APP)

memoria_conversazioni = {}

def elabora_richiesta(user_query: str, chat_id: str = "chat_predefinita") -> dict:
    global memoria_conversazioni
    
    if chat_id not in memoria_conversazioni:# utilizziamo il tuo prompt blindato più recente, non quello vecchio del collega
        istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico specializzato in sistemi HVAC. Hai a disposizione fonti documentali e un Calcolatore Termotecnico.
REGOLA 0 (LINGUA OBBLIGATORIA): DEVI rispondere SEMPRE E SOLO in lingua ITALIANA.
REGOLA 1 (DIVIETO DI ALLUCINAZIONE): Devi SEMPRE invocare uno dei tool PRIMA di rispondere. Non usare calcoli a mente o la tua memoria interna.
REGOLA 2 (DOMANDE MIRATE E PROATTIVITÀ): Se l'utente ti chiede di condizionare un ambiente ma non ti fornisce i kW, DEVI usare 'calcola_fabbisogno_termico'. Se per usare questo tool ti mancano dei dati (metri quadri, numero di persone, temperature o tipo di locale), NON INVENTARLI. Fermati e chiedi esplicitamente all'utente i dati mancanti.
REGOLA 3 (FLUSSO A CASCATA): Una volta calcolati i kW con il tool, devi eseguire un'altra azione: usa 'cerca_catalogo_generico' per cercare nel catalogo un modello che abbia una potenza adatta.
REGOLA 4 (VERIFICA DEL CONTESTO): Quando usi i tool documentali ('cerca_manuali' o 'cerca_sito_web'), leggi il testo estratto. Se non trovi la risposta, ammettilo.
REGOLA 5 (SCELTA NUMERICA CATALOGO): Se il catalogo restituisce un elenco numerato per disambiguare le colonne, mostralo all'utente.""")
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

#5 INIZIALIZZAZIONE GLOBALE E SETUP

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

tools = [cerca_catalogo_specifico, cerca_catalogo_generico, cerca_sito_web, cerca_manuali, calcola_fabbisogno_termico]

# configurazione LangGraph e LLM
# parametri aggiunti per limitare i consumi della cpu e della ram
llm = ChatOllama(model="qwen2.5:3b-instruct-q8_0", temperature=0, num_thread=4, num_ctx=2048)
llm_con_tools = llm.bind_tools(tools, parallel_tool_calls=False)

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