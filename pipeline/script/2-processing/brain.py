import os
import sys
import sqlite3
import time
import pandas as pd
import difflib
import re
import time
from typing import Annotated, List, TypedDict
import operator
from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
import plotly.express as px

# aggiunge la cartella script al path per importare config.py
_script_dir = os.path.dirname(os.path.abspath(__file__))
_cartella_script = os.path.dirname(_script_dir)
sys.path.insert(0, _cartella_script)
from config import CATALOGO_PATH, GOOGLE_API_KEYS, CSV_GRAFICI_PATH, DIR_GRAFICI_SALVATI

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

def trova_colonna_esatta_o_simile(nome_richiesto: str, colonne_disponibili: list[str]) -> str | None:
    if not nome_richiesto:
        return None

    nome_norm = " ".join(str(nome_richiesto).strip().lower().split())

    for col in colonne_disponibili:
        col_norm = " ".join(str(col).strip().lower().split())
        if col_norm == nome_norm:
            return col

    punteggi = {}
    parole_richiesta = re.sub(r'[^a-zA-Z0-9]', ' ', nome_norm).split()

    for col in colonne_disponibili:
        col_norm = re.sub(r'[^a-zA-Z0-9]', ' ', str(col).lower())
        parole_col = col_norm.split()
        score = 0
        for pr in parole_richiesta:
            for pc in parole_col:
                if pr == pc or pr in pc:
                    score += 1
        if score > 0:
            punteggi[col] = score

    if not punteggi:
        return None

    return max(punteggi, key=punteggi.get)


def trova_colonna_modello() -> str | None:
    candidati = [
        "Modello Prodotto",
        "Modello PAL",
        "Modello",
        "Codice Modello"
    ]

    for nome in candidati:
        col = trova_colonna_esatta_o_simile(nome, colonne_catalogo)
        if col:
            return col

    return None


def converti_serie_numerica(serie):
    def converti_valore(val):
        if pd.isna(val):
            return None

        s = str(val).strip()
        if s == "" or s.lower() == "nan":
            return None

        if "," in s:
            s = s.replace(".", "").replace(",", ".")
            return pd.to_numeric(s, errors="coerce")

        if "." in s:
            parti = s.split(".")
            if len(parti) == 2 and parti[1].isdigit():
                if len(parti[1]) == 3 and parti[0].isdigit():
                    s = s.replace(".", "")
                else:
                    return pd.to_numeric(s, errors="coerce")
            else:
                s = s.replace(".", "")

        return pd.to_numeric(s, errors="coerce")

    return serie.apply(converti_valore)

indice_api_corrente = 0

def crea_llm_con_chiave(api_key: str):
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash-lite",
        temperature=0,
        google_api_key=api_key
    )

def ottieni_api_key_corrente():
    global indice_api_corrente
    if not GOOGLE_API_KEYS:
        raise ValueError("Nessuna GOOGLE_API_KEY configurata.")
    return GOOGLE_API_KEYS[indice_api_corrente]

def passa_alla_prossima_api_key():
    global indice_api_corrente
    if indice_api_corrente + 1 >= len(GOOGLE_API_KEYS):
        return False
    indice_api_corrente += 1
    print(f"[LLM] Switch API key -> indice {indice_api_corrente + 1}/{len(GOOGLE_API_KEYS)}")
    return True

def reset_api_key_index():
    global indice_api_corrente
    indice_api_corrente = 0

def errore_quota_google(exc: Exception) -> bool:
    testo = str(exc)
    return (
        "RESOURCE_EXHAUSTED" in testo
        or "429" in testo
        or "GenerateRequestsPerDayPerProjectPerModel-FreeTier" in testo
        or "quota exceeded" in testo.lower()
    )

def invoca_llm_con_failover(input_llm):
    global llm, llm_con_tools, app, indice_api_corrente

    ultimo_errore = None

    while True:
        try:
            return llm.invoke(input_llm)
        except Exception as e:
            ultimo_errore = e

            if not errore_quota_google(e):
                raise

            print(f"[LLM] Quota esaurita sulla chiave corrente: {e}")

            if not passa_alla_prossima_api_key():
                raise ultimo_errore

            llm, llm_con_tools, app = costruisci_motore_llm()
            print("[LLM] Motore ricostruito con la nuova API key")

def invoca_llm_con_tools_failover(messages):
    global llm, llm_con_tools, app, indice_api_corrente

    ultimo_errore = None

    while True:
        try:
            return llm_con_tools.invoke(messages)
        except Exception as e:
            ultimo_errore = e

            if not errore_quota_google(e):
                raise

            print(f"[LLM] Quota esaurita sulla chiave corrente: {e}")

            if not passa_alla_prossima_api_key():
                raise ultimo_errore

            llm, llm_con_tools, app = costruisci_motore_llm()
            print("[LLM] Motore ricostruito con la nuova API key")

def separa_testo_e_istruzioni(testo: str):
    if not testo or not isinstance(testo, str):
        return "", ""

    marker = "ISTRUZIONE PER L'AI"
    if marker in testo:
        parti = testo.split(marker, 1)
        testo_utente = parti[0].strip()
        istruzione_interna = parti[1].strip(" :\n\t")
        return testo_utente, istruzione_interna

    return testo.strip(), ""

def pulisci_risposta_tool_per_utente(testo: str) -> str:
    testo_utente, _ = separa_testo_e_istruzioni(testo)

    if not testo_utente:
        return "Non sono riuscito a generare una risposta utile."

    testo_utente = testo_utente.replace("Calcolo completato:", "").strip()
    testo_utente = testo_utente.replace("SUCCESSO:", "").strip()
    testo_utente = testo_utente.replace("ERRORE:", "Si è verificato un errore:").strip()

    return testo_utente

def esegui_istruzione_interna_tool(istruzione: str) -> str:
    if not istruzione or not isinstance(istruzione, str):
        return ""

    testo = istruzione.strip()

    match_catalogo = re.search(
        r"usa il tool 'cerca_catalogo_generico' con parametro_richiesto='([^']+)', ordinamento='([^']+)', top_n=(\d+), valore_target=([0-9]+(?:\.[0-9]+)?)",
        testo,
        re.IGNORECASE
    )
    if match_catalogo:
        parametro_richiesto = match_catalogo.group(1)
        ordinamento = match_catalogo.group(2)
        top_n = int(match_catalogo.group(3))
        valore_target = float(match_catalogo.group(4))
        return cerca_catalogo_generico.invoke({
            "parametro_richiesto": parametro_richiesto,
            "ordinamento": ordinamento,
            "top_n": top_n,
            "valore_target": valore_target
        })

    return ""

#2 DEFINIZIONE DEI TOOL

@tool
def cerca_catalogo_specifico(modello: str, parametro: str = "Tutti") -> str:
    """Usa questo tool quando l'utente nomina un modello specifico (es. 7AI-183E).
    - modello: il codice del modello.
    - parametro: la grandezza fisica da cercare. Se non specificata, chiedila all'utente."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO_SPECIFICO")
    print(f"[TOOL] Ricerca chirurgica -> Modello: {modello} | Parametro: {parametro}")

    if df_catalogo is None:
        return "Errore: file catalogo non caricato."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    codice_pulito = modello.upper().replace("MODELLO", "").strip()

    df_modello = df_catalogo[
        df_catalogo[colonna_modello].astype(str).str.upper().str.contains(codice_pulito, na=False)
    ]

    if df_modello.empty:
        return f"Modello {codice_pulito} non trovato nel catalogo."

    if parametro == "Tutti" or parametro.strip() == "":
        return (
            f"Modello {codice_pulito} trovato. "
            f"Chiedi all'utente quale parametro vuole conoscere."
        )

    colonna_reale = trova_colonna_esatta_o_simile(parametro, colonne_catalogo)
    if not colonna_reale:
        return (
            f"Il parametro '{parametro}' non esiste nel catalogo. "
            f"Chiedi all'utente di specificare meglio."
        )

    valore = df_modello.iloc[0].get(colonna_reale, "ND")

    return (
        f"Dati tecnici per il modello {codice_pulito}:\n"
        f"- {colonna_reale}: {valore}"
    )

@tool
def cerca_catalogo_generico(parametro_richiesto: str, ordinamento: str = "decrescente", top_n: int = 3, valore_target: float = None) -> str:
    """Usa questo tool per domande analitiche e comparative sul catalogo.
    - parametro_richiesto: nome della colonna da analizzare.
    - ordinamento: 'crescente' o 'decrescente'.
    - top_n: numero di modelli da restituire.
    - valore_target: opzionale, filtra i modelli con valore uguale o superiore a questo numero."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO_GENERICO")
    print(f"[TOOL] Estrazione -> Parametro: {parametro_richiesto}, Ordine: {ordinamento}, Top: {top_n}, Target: {valore_target}")

    if df_catalogo is None:
        return "Errore: file catalogo non caricato."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    colonna_reale = trova_colonna_esatta_o_simile(parametro_richiesto, colonne_catalogo)
    if not colonna_reale:
        opzioni = "\n".join([f"- {c}" for c in colonne_catalogo[:15]])
        return (
            "Nessuna colonna valida trovata. "
            "Chiedi all'utente di riformulare oppure di scegliere una di queste opzioni:\n"
            f"{opzioni}"
        )

    df = df_catalogo.copy()
    df[colonna_reale] = converti_serie_numerica(df[colonna_reale])
    risultato = df.dropna(subset=[colonna_reale])

    if risultato.empty:
        return f"Nessun valore numerico valido trovato per il parametro '{colonna_reale}'."

    if valore_target is not None:
        risultato = risultato[risultato[colonna_reale] >= valore_target]
        risultato = risultato.sort_values(by=colonna_reale, ascending=True)
    else:
        ordine = ordinamento.strip().lower()
        ascending = True if ordine == "crescente" else False
        risultato = risultato.sort_values(by=colonna_reale, ascending=ascending)

    risultato = risultato.head(top_n)

    if risultato.empty:
        return f"Nessun modello trovato per il parametro '{colonna_reale}' con i filtri richiesti."

    righe = []
    for _, row in risultato.iterrows():
        nome_modello = row.get(colonna_modello, "Sconosciuto")
        valore = row.get(colonna_reale, "ND")
        righe.append(f"- Modello {nome_modello}: {colonna_reale} = {valore}")

    return "\n".join(righe)

@tool
def cerca_sito_web(query: str) -> str:
    """Usa questo tool per cercare informazioni dal SITO ZOPPELLARO:
    - descrizioni di prodotti e soluzioni (es. unità per sale operatorie, deumidificatori per piscine, roof-top, recuperatori di calore)
    - categorie applicative (ospedali, piscine, aeroporti, industria, referenze, news)
    - testi marketing e descrizioni commerciali generali.
    Non usarlo per istruzioni di manutenzione, installazione o codici di errore.
    - query: parole chiave della ricerca."""
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_web:
        return "Errore: Database sito non caricato."
        
    docs = db_web.similarity_search(query, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

@tool
def cerca_manuali(query: str) -> str:
    """Usa questo tool per cercare nei MANUALI TECNICI (PDF):
    - procedure di installazione e messa in servizio
    - manutenzione, pulizia, sostituzione filtri
    - codici allarme / errore, significato e possibili cause
    - regolazioni, setpoint, parametri di funzionamento.
    Non usarlo per scegliere il modello dal catalogo o per semplici descrizioni commerciali.
    - query: parole chiave della ricerca."""
    print(f"\n[TOOL] Esecuzione CERCA_MANUALI")
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_manuali:
        return "Errore: Database manuali non caricato."
        
    docs = db_manuali.similarity_search(query, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

@tool
def calcola_fabbisogno_termico(area_mq: float, numero_persone: int, temp_esterna: float, temp_interna: float, tipo_locale: str) -> str:
    """Usa questo tool per calcolare i kW necessari per condizionare una stanza.
    Non chiamare questo tool se l'utente non ha fornito esplicitamente tutti e quattro i valori numerici (mq, persone, temp. esterna, temp. interna).
    - area_mq: metri quadri della stanza.
    - numero_persone: quante persone occupano la stanza.
    - temp_esterna: temperatura in gradi all'esterno (es. 35).
    - temp_interna: temperatura desiderata all'interno (es. 22).
    - tipo_locale: es. 'discoteca', 'ufficio', 'ristorante'."""
    print(f"\n[TOOL] Esecuzione CALCOLA_FABBISOGNO_TERMICO")

    if area_mq <= 0 or numero_persone < 0:
        return "Per calcolare correttamente il fabbisogno termico mi servono metri quadri e numero di persone validi.\nISTRUZIONE PER L'AI: chiedi all'utente i dati mancanti o correggi quelli non validi."

    delta_t = abs(temp_esterna - temp_interna)
    print(f"[TOOL] Dati: {area_mq}mq, {numero_persone} persone, T.Est: {temp_esterna}°, T.Int: {temp_interna}° (Delta: {delta_t}°), locale: {tipo_locale}")

    w_mq = 100
    tipo_locale_low = str(tipo_locale).lower()
    if "discoteca" in tipo_locale_low or "palestra" in tipo_locale_low or "industria" in tipo_locale_low:
        w_mq = 150

    carico_base = area_mq * w_mq

    w_persona = 100
    if "discoteca" in tipo_locale_low or "palestra" in tipo_locale_low:
        w_persona = 250

    carico_persone = numero_persone * w_persona

    moltiplicatore_delta = 1.0
    if delta_t > 10:
        gradi_extra = delta_t - 10
        moltiplicatore_delta = 1.0 + (gradi_extra * 0.05)

    fabbisogno_totale_watt = (carico_base + carico_persone) * moltiplicatore_delta
    fabbisogno_kw = fabbisogno_totale_watt / 1000

    return (
        f"Il fabbisogno termico stimato è di {fabbisogno_kw:.2f} kW.\n"
        f"ISTRUZIONE PER L'AI: usa il tool 'cerca_catalogo_generico' con parametro_richiesto='Potenza frigorifera totale macchina', ordinamento='crescente', top_n=3, valore_target={fabbisogno_kw:.2f}"
    )
    
@tool
def calcola_portata_aria(area_mq: float, numero_persone: int, tipo_locale: str = "") -> str:
    """Usa questo tool per calcolare il fabbisogno di ventilazione (m3/h).
    Non chiamare questo tool se l'utente non ha fornito esplicitamente i valori numerici: passa 0 se un dato non e' disponibile.
    - area_mq: metri quadri (inserisci 0 se non forniti).
    - numero_persone: quantita' di persone (inserisci 0 se non fornite).
    - tipo_locale: es. 'scuola', 'palestra', 'ufficio'. (lascia "" se non specificato)."""
    print(f"\n[TOOL] Esecuzione CALCOLA_PORTATA_ARIA")

    if area_mq <= 0 or numero_persone <= 0:
        print("[TOOL] Dati numerici mancanti rilevati. Blocco dell'esecuzione.")
        return "Per calcolare la portata d'aria mi servono almeno i metri quadri e il numero di persone.\nISTRUZIONE PER L'AI: fermati e chiedi all'utente i dati mancanti."

    if not tipo_locale or tipo_locale.strip() == "":
        tipo_locale = "generico"

    m3h_persona = 40
    tipo_locale_low = tipo_locale.lower()
    if "palestra" in tipo_locale_low or "discoteca" in tipo_locale_low or "ristorante" in tipo_locale_low:
        m3h_persona = 60

    fabbisogno_persone = numero_persone * m3h_persona

    altezza_media = 3.0
    volume = area_mq * altezza_media

    ach = 2.0
    if "scuola" in tipo_locale_low or "ristorante" in tipo_locale_low:
        ach = 4.0
    elif "palestra" in tipo_locale_low or "discoteca" in tipo_locale_low:
        ach = 6.0

    fabbisogno_volumetrico = volume * ach
    portata_finale = max(fabbisogno_persone, fabbisogno_volumetrico)

    print(f"[TOOL] MAX tra Persone ({fabbisogno_persone}) e Volume ({fabbisogno_volumetrico}) = {portata_finale} m3/h")

    return (
        f"La portata d'aria necessaria stimata è di {portata_finale:.2f} m3/h.\n"
        f"ISTRUZIONE PER L'AI: usa il tool 'cerca_catalogo_generico' con parametro_richiesto='Portata Massima', ordinamento='crescente', top_n=3, valore_target={portata_finale:.2f}"
    )

@tool
def calcola_consumo_elettrico(codici_modelli: str, kw_richiesti: float = 0.0) -> str:
    """Usa questo tool per calcolare il consumo elettrico (kW assorbiti) di uno o piu modelli.
    - codici_modelli: i codici dei modelli da analizzare. Per piu modelli, separali con virgola (es. 'modelloA, modelloB').
    - kw_richiesti: (opzionale) kW specificati dall'utente. Se non indicati, lascia 0.0."""
    print(f"\n[TOOL] Esecuzione CALCOLA_CONSUMO_ELETTRICO -> Modelli: {codici_modelli} | Input kW: {kw_richiesti}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    if not codici_modelli or codici_modelli.strip() == "":
        return "Dati incompleti. Chiedi all'utente il codice del modello."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    stringa_pulita = codici_modelli.lower().replace(" e ", ",").replace(" o ", ",").replace(" oppure ", ",")
    lista_codici = [c.strip() for c in stringa_pulita.split(",") if c.strip()]

    col_potenza = trova_colonna_esatta_o_simile("Potenza frigorifera totale macchina", colonne_catalogo)
    eer_col = trova_colonna_esatta_o_simile("EER minimo", colonne_catalogo)
    if not eer_col:
        eer_col = trova_colonna_esatta_o_simile("EER compressori", colonne_catalogo)

    cop_col = trova_colonna_esatta_o_simile("COP minimo", colonne_catalogo)
    if not cop_col:
        cop_col = trova_colonna_esatta_o_simile("COP compressori", colonne_catalogo)

    risultati_finali = []

    for codice_singolo in lista_codici:
        codice_pulito = codice_singolo.upper().replace("MODELLO", "").strip()
        df_modello = df_catalogo[df_catalogo[colonna_modello].astype(str).str.upper().str.contains(codice_pulito, na=False)]

        if df_modello.empty:
            risultati_finali.append(f"Modello {codice_pulito} non trovato nel catalogo.")
            continue

        riga = df_modello.iloc[0]
        risultati = []
        kw_attuali = kw_richiesti

        if kw_attuali <= 0:
            if not col_potenza:
                risultati_finali.append(f"Modello {codice_pulito}: impossibile determinare i kW.")
                continue

            val_potenza = riga.get(col_potenza, 0)
            potenza_num = converti_serie_numerica(pd.Series([val_potenza])).iloc[0]

            if pd.isna(potenza_num) or potenza_num <= 0:
                risultati_finali.append(f"Modello {codice_pulito}: impossibile determinare i kW.")
                continue

            kw_attuali = float(potenza_num)
            risultati.append(f"Nota: calcolo basato su potenza di targa di {kw_attuali:.2f} kW.")

        if eer_col:
            valore_eer = riga.get(eer_col, None)
            eer_float = converti_serie_numerica(pd.Series([valore_eer])).iloc[0]
            if pd.notna(eer_float) and eer_float > 0:
                consumo_freddo = kw_attuali / eer_float
                risultati.append(f"- Raffrescamento (EER {eer_float:.2f}): assorbe circa {consumo_freddo:.2f} kW elettrici.")

        if cop_col:
            valore_cop = riga.get(cop_col, None)
            cop_float = converti_serie_numerica(pd.Series([valore_cop])).iloc[0]
            if pd.notna(cop_float) and cop_float > 0:
                consumo_caldo = kw_attuali / cop_float
                risultati.append(f"- Riscaldamento (COP {cop_float:.2f}): assorbe circa {consumo_caldo:.2f} kW elettrici.")

        if risultati:
            risultati_finali.append(f"Modello {codice_pulito} (Carico: {kw_attuali:.2f} kW):\n" + "\n".join(risultati))
        else:
            risultati_finali.append(f"Modello {codice_pulito}: dati di efficienza validi non trovati.")

    return "\n\n".join(risultati_finali)

@tool
def verifica_prevalenza_canali(codici_modelli: str, pascal_persi_impianto: float = 0.0) -> str:
    """Usa questo tool per verificare se la ventola del modello ha abbastanza prevalenza per superare la perdita di carico dei canali.
    - codici_modelli: codici dei modelli separati da virgola (es. '061-035, 091-051').
    - pascal_persi_impianto: valore in Pascal associato alle parole 'Pascal', 'Pa' o 'perdita di carico' nel messaggio. Se non specificato, lascia 0.0."""
    print(f"\n[TOOL] Esecuzione VERIFICA_PREVALENZA -> Modelli: {codici_modelli} | Pascal: {pascal_persi_impianto}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    if not codici_modelli or codici_modelli.strip() == "":
        return "Dati incompleti. Chiedi all'utente il codice del modello."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    col_prevalenza = trova_colonna_esatta_o_simile("Prevalenza Massima Mandata", colonne_catalogo)
    if not col_prevalenza:
        col_prevalenza = trova_colonna_esatta_o_simile("1 Pressione Massima", colonne_catalogo)

    if not col_prevalenza:
        return "Errore: impossibile trovare la colonna della prevalenza nel catalogo."

    stringa_pulita = codici_modelli.lower().replace(" e ", ",").replace(" o ", ",").replace(" oppure ", ",")
    lista_codici = [p.strip() for p in stringa_pulita.split(",") if p.strip() != ""]

    risultati_finali = []

    for codice_singolo in lista_codici:
        codice_pulito = codice_singolo.upper().replace("MODELLO", "").strip()
        df_modello = df_catalogo[df_catalogo[colonna_modello].astype(str).str.upper().str.contains(codice_pulito, na=False)]

        if df_modello.empty:
            risultati_finali.append(f"Modello {codice_pulito} non trovato.")
            continue

        valore_prev = df_modello.iloc[0].get(col_prevalenza, None)
        prev_float = converti_serie_numerica(pd.Series([valore_prev])).iloc[0]

        if pd.isna(prev_float):
            risultati_finali.append(f"Modello {codice_pulito}: impossibile leggere il valore di prevalenza.")
            continue

        if pascal_persi_impianto <= 0:
            risultati_finali.append(f"Modello {codice_pulito}: ha una prevalenza massima di {prev_float:.2f} Pa (richiesta non specificata).")
        else:
            if prev_float >= pascal_persi_impianto:
                risultati_finali.append(f"Modello {codice_pulito}: COMPATIBILE. Ha {prev_float:.2f} Pa, superiore ai {pascal_persi_impianto:.2f} Pa richiesti.")
            else:
                risultati_finali.append(f"Modello {codice_pulito}: NON COMPATIBILE. Ha solo {prev_float:.2f} Pa, insufficiente per i {pascal_persi_impianto:.2f} Pa richiesti.")

    return "\n\n".join(risultati_finali)

@tool
def consulta_dizionario_catalogo(parola_chiave: str) -> str:
    """Usa questo tool per spiegare il significato di termini tecnici o acronimi (es. 'EER', 'prevalenza', 'portata').
    Non usarlo se l'utente ha gia fornito un codice modello specifico: in quel caso usa 'cerca_catalogo_specifico'.
    - parola_chiave: il termine da cercare."""
    print(f"\n[TOOL] Esecuzione CONSULTA_DIZIONARIO -> Ricerca: '{parola_chiave}'")

    if not parola_chiave or parola_chiave.strip() == "":
        return "Errore: Devi fornire una parola chiave per cercare nel dizionario."

    cartella_corrente = os.path.dirname(os.path.abspath(__file__))
    percorso_file = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

    try:
        with open(percorso_file, "r", encoding="utf-8") as f:
            contenuto = f.read()
    except FileNotFoundError:
        return "Errore: Il file del dizionario non è stato trovato. Avvisa l'utente."
    except UnicodeDecodeError:
        with open(percorso_file, "r", encoding="latin-1") as f:
            contenuto = f.read()

    paragrafi = contenuto.split("\n\n")
    risultati = []
    parola = parola_chiave.strip().lower()

    for p in paragrafi:
        if parola in p.lower():
            risultati.append(p.strip())

    if risultati:
        testo_ritorno = f"DATI ESTRATTI PER '{parola_chiave}':\n"
        for r in risultati[:5]:
            testo_ritorno = testo_ritorno + r + "\n\n"
        return testo_ritorno.strip()

    return f"Nessuna voce trovata per la parola chiave '{parola_chiave}'."


@tool 
def estrai_dati_dinamici(richiesta_utente: str) -> str:
    """Usa questo tool quando l'utente chiede di elaborare i dati in modo complesso o estrarre file personalizzati dal catalogo."""
    global df_catalogo, llm
    print(f"\n[TOOL] Esecuzione ESTRAI_DATI_DINAMICI -> Richiesta: '{richiesta_utente}'")
    
    if df_catalogo is None:
         return "Errore: database catalogo non caricato in memoria."
         
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pipeline_dir = os.path.dirname(os.path.dirname(script_dir))
        cartella_destinazione = os.path.dirname(CSV_GRAFICI_PATH)
        os.makedirs(cartella_destinazione, exist_ok=True)
        path_salvataggio = CSV_GRAFICI_PATH 
        
        df_lavoro = df_catalogo.copy()
        df_lavoro.columns = df_lavoro.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
        colonne_reali = list(df_lavoro.columns)
        
        prompt = f"""
        Scrivi un filtro Pandas in base a questa richiesta:
        {richiesta_utente}

        Lista esatta delle colonne del dataframe df:
        {colonne_reali}

        COMPILA ESATTAMENTE QUESTE DUE RIGHE, senza aggiungerne altre:

        ```python
        colonna_numerica = converti_serie_numerica(df["NOMECOLONNA"])
        dfrisultato = df[colonna_numerica SEGNO NUMERO]
        ```

        REGOLE DI COMPILAZIONE:
        1. NOMECOLONNA deve essere una delle colonne presenti nella lista.
        2. SEGNO deve essere uno tra >, >=, <, <=, ==.
        3. NUMERO deve essere preso dalla richiesta utente.
        4. La variabile finale deve chiamarsi obbligatoriamente dfrisultato.
        5. Non aggiungere spiegazioni.
        6. Non aggiungere print.
        7. Non usare altre variabili finali.
        8. Non usare str.replace(".", "") o conversioni numeriche personalizzate.
        9. Usa obbligatoriamente converti_serie_numerica.
        10. Restituisci solo codice Python valido, preferibilmente dentro un blocco ```python.
        """
        
        risposta_llm = invoca_llm_con_failover(prompt)

        contenuto = risposta_llm.content.strip()
        match = re.search(r"```python\s*(.*?)\s*```", contenuto, re.DOTALL)
        if match:
            codice_pulito = match.group(1).strip()
        else:
            codice_pulito = contenuto

        if codice_pulito.startswith("python"):
            codice_pulito = codice_pulito[len("python"):].strip()
            
        print(f"\n[DEBUG LLM] Codice Pandas generato:\n{codice_pulito}\n")
    
        scatola_sicura = {
            "pd": pd,
            "df": df_lavoro,
            "converti_serie_numerica": converti_serie_numerica,
            "dfrisultato": None
        }

        print(f"[DEBUG TOOL] Colonne disponibili: {colonne_reali}")
        exec(codice_pulito, {}, scatola_sicura)
        
        if "colonna_numerica" in scatola_sicura:
            colonna_debug = scatola_sicura["colonna_numerica"]
            if hasattr(colonna_debug, "notna"):
                print(f"[DEBUG TOOL] Valori numerici validi nella colonna: {int(colonna_debug.notna().sum())}")

        df_finale = scatola_sicura.get("dfrisultato")

        if df_finale is None:
            return "ERRORE: il codice generato non ha creato la variabile 'dfrisultato'."

        if not isinstance(df_finale, pd.DataFrame):
            return "ERRORE: 'dfrisultato' non è un DataFrame Pandas valido."

        print(f"[DEBUG TOOL] Righe trovate: {len(df_finale)}")

        if df_finale.empty:
            return "Nessun modello trovato con i filtri richiesti."

        df_finale.to_csv(path_salvataggio, index=False)
        return f"SUCCESSO: Dati estratti e salvati in data/3-user_interface/dataframe_grafico.csv. Righe trovate: {len(df_finale)}"
        
    except PermissionError:
        return "ERRORE CRITICO: Chiudi il file CSV se aperto in Excel e riprova."
    except Exception as e:
        errore_testo = str(e)

        if "RESOURCE_EXHAUSTED" in errore_testo or "429" in errore_testo or "quota" in errore_testo.lower():
            return "ERRORE QUOTA LLM: hai esaurito la quota gratuita del modello Gemini attualmente in uso. Riprova dopo il reset giornaliero oppure cambia modello/API key."

        return f"ERRORE DI ESECUZIONE PYTHON: {e}"

@tool
def genera_grafico_avanzato(richiesta_utente: str) -> str:
    """Usa questo tool SOLO quando l'utente chiede esplicitamente di creare o mostrare un grafico
    partendo dai dati già estratti nel file CSV generato da estrai_dati_dinamici.
    - richiesta_utente: frase completa dell'utente."""
    print(f"\n[TOOL] Esecuzione GENERA_GRAFICO_AVANZATO -> Richiesta: '{richiesta_utente}'")


    try:
        csv_path = CSV_GRAFICI_PATH

        if not os.path.exists(csv_path):
            return "ERRORE: File dataframe_grafico.csv non trovato. Prima devi estrarre i dati."

        try:
            df_temp = pd.read_csv(csv_path)
        except UnicodeDecodeError:
            df_temp = pd.read_csv(csv_path, encoding="latin-1")

        if df_temp.empty:
            return "ERRORE: Il file CSV esiste ma non contiene righe utili."

        df_temp.columns = (
            df_temp.columns
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        colonne = list(df_temp.columns)

        prompt = f"""
L'utente vuole un grafico basato su un dataframe Pandas già pronto.

Richiesta utente:
{richiesta_utente}

Colonne reali del dataframe df:
{colonne}

REGOLE OBBLIGATORIE:
1. Usa solo 'df' e 'px'.
2. Crea ESATTAMENTE una variabile finale chiamata fig.
3. Non usare print.
4. Non usare fig.show().
5. Non leggere file.
6. Non creare dataframe di esempio.
7. Usa solo colonne realmente presenti.
8. Restituisci solo codice Python valido, preferibilmente dentro ```python.
9. Se la richiesta è ambigua, crea un grafico a barre semplice usando la prima colonna testuale come asse x e la prima colonna numerica come asse y.
10. Se possibile imposta anche un titolo chiaro con fig.update_layout(title="...").

ESEMPI VALIDI:

```python
fig = px.bar(df, x="Modello Prodotto", y="Portata Massima", title="Portata Massima per modello")
```

```python
fig = px.scatter(df, x="Portata Massima", y="Pressione Operativa", color="Grandezza Telaio", title="Portata vs Pressione")
```
"""

        risposta_llm = invoca_llm_con_failover(prompt)

        contenuto = risposta_llm.content.strip()
        match = re.search(r"```python\s*(.*?)\s*```", contenuto, re.DOTALL)
        if match:
            codice_pulito = match.group(1).strip()
        else:
            codice_pulito = contenuto

        if codice_pulito.startswith("python"):
            codice_pulito = codice_pulito[len("python"):].strip()

        print("[DEBUG LLM] Codice Plotly generato:")
        print(codice_pulito)

        scatola_sicura = {
            "df": df_temp,
            "px": px,
            "pd": pd
        }

        exec(codice_pulito, {}, scatola_sicura)

        if "fig" not in scatola_sicura:
            return "ERRORE: Il modello non ha creato la variabile 'fig'."

        os.makedirs(DIR_GRAFICI_SALVATI, exist_ok=True)
        nome_file = f"grafico_{int(time.time())}.html"
        html_path = os.path.join(DIR_GRAFICI_SALVATI, nome_file)

        scatola_sicura["fig"].write_html(html_path, full_html=False, include_plotlyjs="cdn")

        return f"SUCCESSO_GRAFICO::{html_path}"

    except Exception as e:
        errore_testo = str(e)

        if "RESOURCE_EXHAUSTED" in errore_testo or "429" in errore_testo or "quota" in errore_testo.lower():
            return "ERRORE QUOTA LLM: hai esaurito la quota gratuita del modello Gemini attualmente in uso. Riprova dopo il reset giornaliero oppure cambia modello/API key."

        return f"ERRORE: {e}"
    
#3 FUNZIONI DI LANGGRAPH E LOGICA AI

def call_model(state: AgentState):
    print("\nL'intelligenza artificiale sta analizzando i dati e generating la risposta...")
    messages = state["messages"]
    response = invoca_llm_con_tools_failover(messages)
    
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

def route_after_tool(state: AgentState) -> str:
    return "agent"

#4 FUNZIONI DI INTERFACCIA (APP)

memoria_conversazioni = {}
stato_grafici = {}

def aggiorna_stato_grafico_da_followup(user_query: str, stato: dict) -> bool:
    testo = user_query.lower().strip()
    modificato = False

    if "grafico" in testo or "barre" in testo:
        stato["tipo"] = "bar"
        modificato = True

    if "ordina" in testo and ("alto" in testo or "decresc" in testo):
        stato["ordinamento"] = "decrescente"
        modificato = True
    elif "ordina" in testo and ("basso" in testo or "cresc" in testo):
        stato["ordinamento"] = "crescente"
        modificato = True

    match_top = re.search(r"(primi|solo i primi)\s+(\d+)", testo)
    if match_top:
        stato["top_n"] = int(match_top.group(2))
        modificato = True

    if "pressione spinta massima" in testo:
        stato["y"] = "Pressione Spinta Massima"
        modificato = True

    if "modello" in testo or "modelli" in testo:
        stato["x"] = "Modello Prodotto"
        modificato = True

    if modificato:
        stato["grafico_presente"] = True

    return modificato

def elabora_richiesta(user_query: str, chat_id: str = "chat_predefinita") -> dict:
    global memorie_conversazioni, llm, llm_con_tools, app

    if chat_id not in memoria_conversazioni:
        istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico HVAC. Devi rispettare RIGOROSAMENTE questo albero decisionale (IF/THEN):

1. IF l'utente chiede un modello per CONDIZIONARE, RAFFRESCARE o RISCALDARE un ambiente:
- Controlla se hai: 1. Metri quadri, 2. Numero persone, 3. Temp. Esterna, 4. Temp. Interna.
- SE l'utente fa un follow-up, recupera i dati invariati dalla cronologia.
- SE CONTINUA A MANCARE UN DATO: Fermati e chiedilo.
- SE HAI TUTTI I DATI: Usa prima 'calcola_fabbisogno_termico'. Se il tool restituisce una "ISTRUZIONE PER L'AI", esegui subito il passo successivo richiesto e completa la selezione del modello prima di rispondere all'utente.

2. IF l'utente chiede un modello per VENTILARE o garantire il RICAMBIO D'ARIA:
- Controlla se hai: 1. Metri quadri, 2. Numero persone, 3. Tipo di locale.
- SE l'utente sta aggiornando i numeri, recupera i dati invariati dalla chat.
- SE MANCA UN DATO: Fermati e chiedilo.
- SE HAI TUTTI I DATI: Usa prima 'calcola_portata_aria'. Se il tool restituisce una "ISTRUZIONE PER L'AI", esegui subito il passo successivo richiesto e completa la selezione del modello prima di rispondere all'utente. Mostra SEMPRE almeno 3 modelli se disponibili.

3. IF l'utente chiede il CONSUMO ELETTRICO o quale modello CONVIENE/CONSUMA MENO:
- Usa 'calcola_consumo_elettrico'. NON cercare l'efficienza prima, il tool la troverà da solo.

4. IF l'utente chiede la PREVALENZA o la COMPATIBILITÀ CON I CANALI (perdita di carico / Pascal):
- Usa 'verifica_prevalenza_canali' passando i modelli e i Pascal richiesti.

5. IF l'utente chiede un dato tecnico di un MODELLO SPECIFICO:
- Se nella richiesta è presente un codice modello esatto (es. 091-051), usa ESCLUSIVAMENTE 'cerca_catalogo_specifico'.
- Non usare 'consulta_dizionario_catalogo' se la domanda contiene il codice di un modello.

6. IF l'utente chiede informazioni su PRODOTTI o SOLUZIONI ZOPPELLARO, o su applicazioni generali (es. "climatizzatori per sale operatorie", "soluzioni per piscine coperte", "recuperatori di calore", "condizionatori roof-top", "referenze", "che cosa produce Zoppellaro"):
- Usa ESCLUSIVAMENTE il tool 'cerca_sito_web'.
- Riassumi in modo chiaro le informazioni trovate, citando i prodotti o le linee rilevanti.

7. IF l'utente fa domande su MANUTENZIONE, FILTRI, INSTALLAZIONE, AVVIAMENTO, CODICI DI ERRORE/ALLARME o regolazioni pratiche (es. "come si installa", "come si puliscono i filtri", "errore E1", "come impostare la temperatura"):
- Usa ESCLUSIVAMENTE il tool 'cerca_manuali'.
- Non usare 'cerca_sito_web' per queste domande.

8. IF l'utente chiede un grafico, diagramma o tabella basati direttamente su dati già estratti in CSV:
- Usa il tool 'genera_grafico_avanzato'.

9. IF l'utente chiede estrazioni dati particolari, incroci complessi, o usa parole come "crea un nuovo file", "estrai i dati", "salva CSV":
- Usa il tool 'estrai_dati_dinamici'. Passagli la richiesta completa dell'utente.

10. IF l'utente chiede prima un'estrazione e poi anche una visualizzazione dei dati:
- Prima usa 'estrai_dati_dinamici'.
- Solo in un messaggio successivo dell'utente usa 'genera_grafico_avanzato'.

11. IF l'utente chiede di creare un GRAFICO o PLOTTARE i dati che sono stati estratti nel CSV:
- Usa il tool 'genera_grafico_avanzato' passando la frase intera dell'utente.
- Usa questo tool SOLO se esistono già dati estratti nel CSV.
- Non descrivere il grafico a parole se puoi generarlo davvero.

12. IF esiste già un grafico creato nella chat corrente e l'utente fa un follow-up come "ordina", "mostrami solo i primi 5", "cambia asse", "rifallo a barre", "fallo orizzontale":
- Interpreta la richiesta come una MODIFICA del grafico corrente.
- Non usare 'cerca_catalogo_generico' se la richiesta è chiaramente una modifica del grafico.
- Rigenera il grafico aggiornato partendo dai dati già estratti.

REGOLE GLOBALI:
- Rispondi SOLO in Italiano.
- NON inventare parametri. Se non li sai, chiedili.
- DIVIETO DI CALCOLO A VUOTO: se l'utente fa una domanda puramente discorsiva e NON fornisce numeri (kW, mq, persone, Pascal), ti è ASSOLUTAMENTE VIETATO usare i tool di calcolo (termico, aria, elettrico, prevalenza). Usa solo il dizionario o rispondi a parole.
- DIVIETO DI JSON: Non rispondere mai mostrando codice JSON grezzo all'utente.
- REGOLA TOOL: Chiama un solo tool per volta.
- ECCEZIONE CONTROLLATA: se il risultato del tool contiene la stringa "ISTRUZIONE PER L'AI", non fermarti; usa quella istruzione come guida per chiamare ESATTAMENTE il tool successivo necessario.
- REGOLA ANTI-LOOP: dopo il tool successivo richiesto dall'istruzione interna, formula la risposta finale per l'utente e fermati. Non eseguire catene extra o verifiche aggiuntive.
- REGOLA DI PRIVACY: Non menzionare mai nomi di cartelle, percorsi di file o dettagli del sistema operativo nelle tue risposte.
- REGOLA 'ISTRUZIONE PER L'AI': quando ricevi una risposta da un tool che contiene la stringa "ISTRUZIONE PER L'AI", non mostrare quella parte all'utente; usala solo come guida interna per decidere il prossimo tool da chiamare.""")
        memoria_conversazioni[chat_id] = [istruzioni_di_sistema]
    
    if chat_id not in stato_grafici:
        stato_grafici[chat_id] = {
            "csv_path": CSV_GRAFICI_PATH,
            "tipo": None,
            "x": None,
            "y": None,
            "filtro_testuale": None,
            "ordinamento": None,
            "top_n": None,
            "titolo": None,
            "grafico_presente": False
        }

    memoria_conversazioni[chat_id].append(HumanMessage(content=user_query))

    match_modello_specifico = re.search(r"\b\d{3}-\d{3}\b", user_query)
    richiesta_visiva = any(parola in user_query.lower() for parola in ["grafico", "diagramma", "tabella", "analisi visiva"])

    stato_grafico_chat = stato_grafici.get(chat_id)
    followup_grafico = False

    if stato_grafico_chat and stato_grafico_chat.get("grafico_presente"):
        followup_grafico = aggiorna_stato_grafico_da_followup(user_query, stato_grafico_chat)

    if followup_grafico:
        parti_richiesta = []

        if stato_grafico_chat.get("tipo") == "bar":
            parti_richiesta.append("Crea un grafico a barre")

        if stato_grafico_chat.get("x"):
            parti_richiesta.append(f"con asse X '{stato_grafico_chat['x']}'")

        if stato_grafico_chat.get("y"):
            parti_richiesta.append(f"e asse Y '{stato_grafico_chat['y']}'")

        if stato_grafico_chat.get("ordinamento"):
            parti_richiesta.append(f"ordinato in modo {stato_grafico_chat['ordinamento']}")

        if stato_grafico_chat.get("top_n"):
            parti_richiesta.append(f"mostrando solo i primi {stato_grafico_chat['top_n']} valori")

        richiesta_grafico = " ".join(parti_richiesta).strip()

        esito_grafico = genera_grafico_avanzato.invoke({"richiesta_utente": richiesta_grafico})

        if esito_grafico.startswith("SUCCESSO_GRAFICO::"):
            path_grafico = esito_grafico.split("::", 1)[1].strip()
            return {
                "testo": "Ho aggiornato il grafico in base alla tua richiesta.",
                "azioni": ["genera_grafico_avanzato"],
                "dati_visivi": {
                    "tipo": "grafico_html_file",
                    "path": path_grafico
                }
            }

        return {
            "testo": pulisci_risposta_tool_per_utente(esito_grafico),
            "azioni": ["genera_grafico_avanzato"]
        }

    messaggi_completi = memoria_conversazioni[chat_id]
    messaggi_per_llm = [messaggi_completi[0]]

    MAX_MESSAGGI_CONTESTO = 12

    if len(messaggi_completi) > MAX_MESSAGGI_CONTESTO + 1:
        messaggi_per_llm.extend(messaggi_completi[-MAX_MESSAGGI_CONTESTO:])
    else:
        messaggi_per_llm.extend(messaggi_completi[1:])

    current_state = {"messages": messaggi_per_llm}

    ultimo_errore = None

    while True:
        try:
            start_time = time.time()
            result = app.invoke(current_state, recursion_limit=15)
            end_time = time.time()
            tempo_trascorso = end_time - start_time

            if ultimo_errore is None:
                print(f"[DEBUG TEMPO] Tempo di risposta: {tempo_trascorso:.2f} secondi")
            else:
                print(f"[DEBUG TEMPO] Tempo di risposta dopo switch: {tempo_trascorso:.2f} secondi")
            break

        except Exception as e:
            ultimo_errore = e

            if errore_quota_google(e):
                print(f"[LLM] Quota esaurita sulla chiave corrente: {e}")

                if passa_alla_prossima_api_key():
                    llm, llm_con_tools, app = costruisci_motore_llm()
                    print("[LLM] Motore ricostruito con la nuova API key")
                    continue

                return {
                    "testo": "Quota Google esaurita su tutte le API key configurate per oggi.",
                    "azioni": []
                }

            return {
                "testo": f"Si è verificato un errore nel motore: {e}",
                "azioni": []
            }

    nuovi_messaggi = result["messages"]
    risposta_grezza = ""
    istruzione_interna_tool = ""

    for msg in reversed(nuovi_messaggi):
        if hasattr(msg, "content") and isinstance(msg.content, str) and msg.content.strip() != "":
            testo = msg.content.strip()
            testo_utente, testo_istruzioni = separa_testo_e_istruzioni(testo)

            if testo_istruzioni:
                istruzione_interna_tool = testo_istruzioni

            if testo_utente:
                risposta_grezza = testo_utente
                break

    tool_usati = []
    for msg in nuovi_messaggi:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tool in msg.tool_calls:
                nome_tool = tool.get("name")
                if nome_tool and nome_tool not in tool_usati:
                    tool_usati.append(nome_tool)

    if istruzione_interna_tool:
        risultato_tool_successivo = esegui_istruzione_interna_tool(istruzione_interna_tool)
        if risultato_tool_successivo:
            risposta_grezza = risultato_tool_successivo
            if "cerca_catalogo_generico" not in tool_usati:
                tool_usati.append("cerca_catalogo_generico")

    if "calcola_fabbisogno_termico" in tool_usati and "cerca_catalogo_generico" not in tool_usati:
        risposta_grezza = (risposta_grezza + "\nNon sono ancora riuscito a completare la selezione automatica del modello dal catalogo.").strip()

    prompt_finale = f"""
    Sei un assistente tecnico HVAC.

    Devi trasformare il risultato di un tool in una risposta finale per l'utente, in italiano naturale.

    REGOLE OBBLIGATORIE:
    - Non mostrare mai istruzioni interne, ragionamenti, nomi di tool o frasi come "usa il tool", "ISTRUZIONE PER L'AI", "parametro_richiesto", "valore_target".
    - Non parlare mai come se stessi programmando un workflow.
    - Non menzionare JSON, campi, variabili, API, file interni o percorsi.
    - Se il testo contiene un risultato numerico, spiegalo in modo semplice e diretto.
    - Se il testo contiene un elenco di modelli, presentalo come consiglio tecnico finale, senza dire che non sei riuscito a completare il processo.
    - Se il testo contiene righe del tipo "- Modello ...", trasformale in elenco puntato leggibile.
    - Se il testo contiene un elenco di modelli, cita chiaramente i modelli trovati e il parametro confrontato.
    - Se il testo contiene un errore, trasformalo in un messaggio chiaro per l'utente.
    - Mantieni solo le informazioni utili all'utente finale.
    - Non inventare dati non presenti.
    - Non aggiungere premesse inutili.
    - Se il risultato è già chiaro, miglioralo soltanto nello stile.
    - Se sono presenti modelli compatibili o idonei, non chiedere di nuovo dati già forniti dall'utente.
    - Se il risultato contiene 3 modelli, presentali in elenco puntato.

    Domanda utente:
    {user_query}

    Risultato tool pulito:
    {risposta_grezza}
    """

    try:
        risposta_assistente = invoca_llm_con_failover([
            SystemMessage(content="Riscrivi in italiano naturale la risposta tecnica per l'utente finale."),
            HumanMessage(content=prompt_finale)
        ]).content.strip()
    except Exception:
        risposta_assistente = pulisci_risposta_tool_per_utente(risposta_grezza)

    memoria_conversazioni[chat_id].append(AIMessage(content=risposta_assistente))

    dati_da_esportare = None

    for msg in reversed(nuovi_messaggi):
        if hasattr(msg, "content") and isinstance(msg.content, str):
            contenuto_msg = msg.content.strip()
            if contenuto_msg.startswith("SUCCESSO_GRAFICO::"):
                percorso_file = contenuto_msg.split("SUCCESSO_GRAFICO::", 1)[1].strip()
                dati_da_esportare = {"tipo": "grafico_html_file", "path": percorso_file}
                if risposta_assistente == contenuto_msg:
                    risposta_assistente = "Grafico generato correttamente."
    stato_grafici[chat_id]["grafico_presente"] = True
    stato_grafici[chat_id]["tipo"] = "bar"
    stato_grafici[chat_id]["x"] = "Modello Prodotto"
    stato_grafici[chat_id]["y"] = "Pressione Spinta Massima"
    stato_grafici[chat_id]["csv_path"] = CSV_GRAFICI_PATH

    return {
        "testo": risposta_assistente,
        "azioni": tool_usati,
        "dati_visivi": dati_da_esportare
    }


#5 INIZIALIZZAZIONE GLOBALE E SETUP

print("Inizializzazione sistema in corso...")
embeddings_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

db_catalogo = carica_database("chroma_db_catalogo", "catalogo", embeddings_model)
db_web = carica_database("chroma_db_zoppellaro", "sito_web", embeddings_model)
db_manuali = carica_database("chroma_db_knowledge_base_pdf", "manuali", embeddings_model)

try:
    if not os.path.exists(CATALOGO_PATH):
        print(f"Errore caricamento catalogo da '{CATALOGO_PATH}': file non trovato")
        df_catalogo = None
        colonne_catalogo = []
    else:
        if CATALOGO_PATH.lower().endswith(".xlsx"):
            df_catalogo = pd.read_excel(CATALOGO_PATH)
        else:
            try:
                df_catalogo = pd.read_csv(
                    CATALOGO_PATH,
                    sep=None,
                    engine="python",
                    encoding="utf-8"
                )
            except UnicodeDecodeError:
                df_catalogo = pd.read_csv(
                    CATALOGO_PATH,
                    sep=None,
                    engine="python",
                    encoding="latin-1"
                )

        df_catalogo.columns = (
            df_catalogo.columns
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        colonne_catalogo = []
        for col in df_catalogo.columns:
            colonna_stringa = str(col).strip().lower()
            if not colonna_stringa.endswith("unit"):
                colonne_catalogo.append(col)

        print(f"[CATALOGO] File caricato: {CATALOGO_PATH}")
        print(f"[CATALOGO] Righe: {len(df_catalogo)} | Colonne: {len(colonne_catalogo)}")

except Exception as e:
    print(f"Errore caricamento catalogo da '{CATALOGO_PATH}': {e}")
    df_catalogo = None
    colonne_catalogo = []

tools = [cerca_catalogo_specifico, 
         cerca_catalogo_generico, 
         cerca_sito_web, #cerca_manuali, 
         calcola_fabbisogno_termico, 
         calcola_portata_aria, 
         calcola_consumo_elettrico, 
         verifica_prevalenza_canali,
         consulta_dizionario_catalogo,
         estrai_dati_dinamici,
         genera_grafico_avanzato]

# configurazione LangGraph e LLM
def costruisci_motore_llm():
    llm_locale = crea_llm_con_chiave(ottieni_api_key_corrente())
    llm_con_tools_locale = llm_locale.bind_tools(tools)
    tool_node_locale = ToolNode(tools)

    workflow_locale = StateGraph(AgentState)
    workflow_locale.add_node("agent", call_model)
    workflow_locale.add_node("tools", tool_node_locale)
    workflow_locale.set_entry_point("agent")
    workflow_locale.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
    workflow_locale.add_conditional_edges("tools", route_after_tool, {"agent": "agent", "end": END})

    app_locale = workflow_locale.compile()
    return llm_locale, llm_con_tools_locale, app_locale

llm, llm_con_tools, app = costruisci_motore_llm()