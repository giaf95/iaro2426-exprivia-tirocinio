import os
import sys
import sqlite3
import time
import pandas as pd
import difflib
import re
from typing import Annotated, List, TypedDict
import operator

_script_dir = os.path.dirname(os.path.abspath(__file__))
_cartella_script = os.path.dirname(_script_dir)
if _cartella_script not in sys.path:
    sys.path.insert(0, _cartella_script)

from config import CATALOGO_PATH, GOOGLE_API_KEY, GROQ_API_KEY, CSV_GRAFICI_PATH
import dspy
from litellm import query
from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
import plotly.express as px
from langchain_groq import ChatGroq 

os.environ["GROQ_API_KEY"] = GROQ_API_KEY

motore_llama = dspy.LM('groq/llama-3.3-70b-versatile', api_key=GROQ_API_KEY)
dspy.configure(lm=motore_llama)

class AgentState(TypedDict):
    messages: Annotated[List, operator.add]


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
    return pd.to_numeric(
        serie.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip(),
        errors="coerce"
    )

#2 DEFINIZIONE DEI TOOL
class mappa_catalogo(dspy.Signature):
    """Trova la colonna esatta del database che meglio risponde al parametro o concetto cercato."""
    parametro_richiesto = dspy.InputField(desc="Cosa vuole sapere l'utente (es. 'quanto pesa', 'rumorosità', 'potenza')")
    colonne_disponibili = dspy.InputField(desc="Lista in Python delle colonne reali presenti nel file CSV")
    colonna_esatta = dspy.OutputField(desc="Copia ESATTAMENTE il nome della colonna corretta. Se nessuna colonna ha a che fare con la richiesta, scrivi 'NESSUNA'.") 
mappatore_colonne = dspy.Predict(mappa_catalogo)

@tool
def cerca_catalogo_specifico(modello: str, parametro: str = "Tutti") -> str:
    """Usa questo tool quando l'utente nomina un modello specifico (es. 7AI-183E).
    - modello: il codice del modello.
    - parametro: la frase, la domanda o la grandezza fisica cercata dall'utente."""
    
    print(f"\n[TOOL DSPy] Esecuzione CERCA_CATALOGO_SPECIFICO")
    print(f"[TOOL DSPy] Ricerca -> Modello: {modello} | Parametro: {parametro}")

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

    lista_colonne = list(colonne_catalogo)
    print(f"[DSPy] Traduzione semantica del parametro '{parametro}' in corso...")
    
    risultato_dspy = mappatore_colonne(
        parametro_richiesto=parametro,
        colonne_disponibili=str(lista_colonne)
    )
    
    colonna_reale = risultato_dspy.colonna_esatta.strip()
    print(f"[DSPy] DSPy ha mappato la richiesta sulla colonna: '{colonna_reale}'")

    if colonna_reale == "NESSUNA" or colonna_reale not in lista_colonne:
        return (
            f"Il parametro o concetto '{parametro}' non sembra avere una corrispondenza nei dati tecnici del modello. "
            f"Chiedi all'utente di specificare meglio."
        )
    valore = df_modello.iloc[0].get(colonna_reale, "ND")

    return (
        f"Dati tecnici per il modello {codice_pulito}:\n"
        f"- {colonna_reale}: {valore}"
    )
def metrica_valutazione_catalogo(example, pred, trace= None):
    risposta_Llma = pred.colonna_esatta.strip()
    if risposta_Llma == "NESSUNA":
        return True
    if risposta_Llma == example.colonne_disponibili:
        return True
    print(f"[METRICA Fallita] L'IA ha inventato la colonna: '{risposta_Llma}'")
    return False

@tool
def cerca_catalogo_generico(parametro_richiesto: str, ordinamento: str = "decrescente", top_n: int = 3, valore_target: float = None) -> str:
    """Usa questo tool per domande analitiche e comparative sul catalogo.
    - parametro_richiesto: nome della colonna da analizzare.
    - ordinamento: 'crescente' o 'decrescente'.
    - top_n: numero di modelli da restituire.
    - valore_target: opzionale, filtra i modelli con valore uguale o superiore a questo numero."""
    
    print(f"\n[TOOL DSPy] Esecuzione CERCA_CATALOGO_GENERICO")
    print(f"[TOOL DSPy] Estrazione -> Parametro: {parametro_richiesto}, Ordine: {ordinamento}, Top: {top_n}, Target: {valore_target}")

    if df_catalogo is None:
        return "Errore: file catalogo non caricato."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    lista_colonne = list(colonne_catalogo)
    print(f"[DSPy] Traduzione semantica del parametro '{parametro_richiesto}' in corso...")
    
    risultato_dspy = mappatore_colonne(
        parametro_richiesto=parametro_richiesto,
        colonne_disponibili=str(lista_colonne)
    )
    
    colonna_reale = risultato_dspy.colonna_esatta.strip()
    print(f"[DSPy] DSPy ha mappato la richiesta sulla colonna: '{colonna_reale}'")

    if colonna_reale == "NESSUNA" or colonna_reale not in lista_colonne:
        opzioni = "\n".join([f"- {c}" for c in colonne_catalogo[:15]])
        return (
            f"Il parametro '{parametro_richiesto}' non è stato trovato nel database.\n"
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


class OttimizzaQueryRicerca(dspy.Signature):
    """Trasforma una domanda colloquiale in una stringa di ricerca perfetta per un database vettoriale."""
    domanda_utente = dspy.InputField(desc="La domanda confusa o colloquiale dell'utente")
    query_ottimizzata = dspy.OutputField(desc="Una stringa contenente solo le parole chiave tecniche essenziali, codici di errore o nomi di procedure. Niente articoli o convenevoli.")
ottimizzatore_ricerca = dspy.Predict(OttimizzaQueryRicerca)

@tool
def cerca_sito_web(query: str) -> str:
    """Usa questo tool per cercare procedure passo-passo, guide all'installazione, troubleshooting e codici di errore.
    - query: la frase o domanda originale dell'utente."""
    
    print(f"\n[TOOL DSPy] Esecuzione CERCA_SITO_WEB")
    print(f"[TOOL DSPy] Frase originale in ingresso: '{query}'")
    
    if not db_web:
        return "Errore: Database sito non caricato."
    
    risultato_dspy = ottimizzatore_ricerca(domanda_utente=query)
    query_pulita = risultato_dspy.query_ottimizzata.strip()
    print(f"[DSPy] Frase ottimizzata per il Database: '{query_pulita}'")
    
    # --- 2. RICERCA NEL DATABASE VETTORIALE ---
    docs = db_web.similarity_search(query_pulita, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL DSPy] Estratti {len(docs)} documenti.")
    
    return testo_finale

def metrica_sito_web(example, pred, trace=None):
    query_generata = pred.query_ottimizzata.lower()
    numero_parole = len(query_generata.split())
    if numero_parole > 12:
        print(f"[METRICA Fallita] Query troppo lunga ({numero_parole} parole).")
        return False
    parole_vietate = ["ciao", "salve", "per favore", "vorrei", "sapere", "potresti", "dirmi"]
    for parola in parole_vietate:
        if parola in query_generata:
            print(f"[METRICA Fallita] La query contiene parole colloquiali vietate: '{parola}'")
            return False
    return True

@tool
def cerca_manuali(query: str) -> str:
    """Usa questo tool per trovare informazioni commerciali, contatti, o ambienti di applicazione (es. sale operatorie, ospedali, uso industriale).
    Non usarlo per dati tecnici numerici o modelli specifici.
    - query: parole chiave della ricerca."""
    print(f"\n[TOOL] Esecuzione CERCA_MANUALI")
    print(f"[TOOL] Query in ingresso: '{query}'")
    
    if not db_manuali:
        return "Errore: Database manuali non caricato."
        
    docs = db_manuali.similarity_search(query, k=5)
    testo_finale = "\n".join([d.page_content for d in docs])
    print(f"[TOOL] Estratti {len(docs)} documenti.")
    return testo_finale

class ClassificaCaricoTermico(dspy.Signature):
    """Classifica il tipo di ambiente descritto dall'utente per determinare il corretto carico termico."""
    descrizione_ambiente = dspy.InputField(desc="Come l'utente ha descritto il locale (es. 'sala pesi', 'club', 'ufficio', 'ristorante')")
    categoria_assegnata = dspy.OutputField(desc="Rispondi SOLO con una di queste tre etichette esatte: 'STANDARD' (per uffici, case, negozi), 'ALTO_CARICO' (per ristoranti, industrie), 'ESTREMO' (per palestre, discoteche, sale da ballo).")
classificatore_locali = dspy.Predict(ClassificaCaricoTermico)

@tool
def calcola_fabbisogno_termico(area_mq: float, numero_persone: int, temp_esterna: float, temp_interna: float, tipo_locale: str) -> str:
    """Usa questo tool per calcolare i kW necessari per condizionare una stanza.
    Non chiamare questo tool se l'utente non ha fornito esplicitamente tutti e quattro i valori numerici (mq, persone, temp. esterna, temp. interna).
    - area_mq: metri quadri della stanza.
    - numero_persone: quante persone occupano la stanza.
    - temp_esterna: temperatura in gradi all'esterno (es. 35).
    - temp_interna: temperatura desiderata all'interno (es. 22).
    - tipo_locale: descrizione testuale dell'ambiente."""
    
    print(f"\n[TOOL DSPy] Esecuzione CALCOLA_FABBISOGNO_TERMICO")

    if area_mq <= 0 or numero_persone < 0:
        return "ISTRUZIONE PER L'AI: Dati incompleti o non validi. Chiedi all'utente metri quadri e numero persone corretti."

    delta_t = abs(temp_esterna - temp_interna)
    print(f"[TOOL DSPy] Dati: {area_mq}mq, {numero_persone} persone, Delta: {delta_t}°")

    print(f"[DSPy] Classificazione semantica per: '{tipo_locale}'")
    risultato_dspy = classificatore_locali(descrizione_ambiente=tipo_locale)
    categoria = risultato_dspy.categoria_assegnata.strip().upper()
    print(f"[DSPy] L'ambiente è stato classificato come: {categoria}")

    if categoria == "ESTREMO":
        w_mq = 150
        w_persona = 250
    elif categoria == "ALTO_CARICO":
        w_mq = 120
        w_persona = 150
    else: # STANDARD o default
        w_mq = 100
        w_persona = 100

    carico_base = area_mq * w_mq
    carico_persone = numero_persone * w_persona
    moltiplicatore_delta = 1.0
    if delta_t > 10:
        gradi_extra = delta_t - 10
        moltiplicatore_delta = 1.0 + (gradi_extra * 0.05)

    fabbisogno_totale_watt = (carico_base + carico_persone) * moltiplicatore_delta
    fabbisogno_kw = fabbisogno_totale_watt / 1000

    return (
        f"Calcolo completato: {fabbisogno_kw:.2f} kW. "
        f"ISTRUZIONE PER L'AI: Ora usa il tool 'cerca_catalogo_generico'. "
        f"Inserisci come parametro_richiesto ESATTAMENTE 'Potenza frigorifera totale macchina' "
        f"e inserisci {fabbisogno_kw:.2f} nel campo 'valore_target'."
    )
def metrica_categoria_termica(example, pred, trace=None):
    categoria_generata = pred.categoria_assegnata.strip().upper()
    categorie_ammesse = ["ESTREMO", "ALTO_CARICO", "STANDARD"]
    
    if categoria_generata in categorie_ammesse:
        return True
    print(f"[METRICA Fallita] L'IA si è inventata la categoria: '{categoria_generata}'")
    return False
    
class ClassificaVentilazioneLocale(dspy.Signature):
    """Classifica il livello di ricambio d'aria (ACH) e m3/h per persona in base al locale."""
    descrizione_locale = dspy.InputField(desc="Descrizione dell'ambiente data dall'utente (es. 'aula', 'trattoria', 'sala pesi', 'ufficio')")
    categoria_assegnata = dspy.OutputField(desc="Rispondi SOLO con: 'STANDARD' (case, uffici), 'MEDIO_CARICO' (scuole, aule), 'AFFOLLATO_SEDUTI' (ristoranti, mense), 'AFFOLLATO_ATTIVO' (palestre, discoteche).")
classificatore_ventilazione = dspy.Predict(ClassificaVentilazioneLocale)

@tool
def calcola_portata_aria(area_mq: float, numero_persone: int, tipo_locale: str = "") -> str:
    """Usa questo tool per calcolare il fabbisogno di ventilazione (m3/h).
    Non chiamare questo tool se l'utente non ha fornito esplicitamente i valori numerici: passa 0 se un dato non e' disponibile.
    - area_mq: metri quadri (inserisci 0 se non forniti).
    - numero_persone: quantita' di persone (inserisci 0 se non fornite).
    - tipo_locale: descrizione dell'ambiente."""
    
    print(f"\n[TOOL DSPy] Esecuzione CALCOLA_PORTATA_ARIA")

    if area_mq <= 0 or numero_persone <= 0:
        print("[TOOL DSPy] Dati numerici mancanti rilevati. Blocco dell'esecuzione.")
        return "ISTRUZIONE PER L'AI: Dati incompleti. Fermati e chiedi all'utente i metri quadri e il numero di persone."

    if not tipo_locale or tipo_locale.strip() == "":
        tipo_locale = "generico"

    print(f"[DSPy] Classificazione ventilazione per: '{tipo_locale}'")
    risultato_dspy = classificatore_ventilazione(descrizione_locale=tipo_locale)
    categoria = risultato_dspy.categoria_assegnata.strip().upper()
    print(f"[DSPy] Il locale è stato classificato come: {categoria}")

    if categoria == "AFFOLLATO_ATTIVO":
        m3h_persona = 60
        ach = 6.0
    elif categoria == "AFFOLLATO_SEDUTI":
        m3h_persona = 60
        ach = 4.0
    elif categoria == "MEDIO_CARICO":
        m3h_persona = 40
        ach = 4.0
    else:
        m3h_persona = 40
        ach = 2.0

    fabbisogno_persone = numero_persone * m3h_persona
    altezza_media = 3.0
    volume = area_mq * altezza_media
    fabbisogno_volumetrico = volume * ach
    
    portata_finale = max(fabbisogno_persone, fabbisogno_volumetrico)

    print(f"[TOOL DSPy] MAX tra Persone ({fabbisogno_persone}) e Volume ({fabbisogno_volumetrico}) = {portata_finale} m3/h")

    return (
        f"Calcolo completato: {portata_finale:.2f} m3/h. "
        f"ISTRUZIONE PER L'AI: Ora usa il tool 'cerca_catalogo_generico'. "
        f"Parametro: 'Portata Massima'. Target: {portata_finale:.2f}."
    )
def metrica_categoria_ventilazione(example, pred, trace=None):
    categoria_generata = pred.categoria_assegnata.strip().upper()
    categorie_ammesse = [
        "STANDARD", 
        "MEDIO_CARICO", 
        "AFFOLLATO_SEDUTI", 
        "AFFOLLATO_ATTIVO"
    ]
    if categoria_generata in categorie_ammesse:
        return True
    print(f"[METRICA Fallita] L'IA si è inventata la categoria aria: '{categoria_generata}'")
    return False

class EstraiListaModelli(dspy.Signature):
    """Estrae un elenco pulito di codici modello da una frase discorsiva o disordinata."""
    testo_input = dspy.InputField(desc="Testo fornito dall'agente contenente uno o più modelli")
    modelli_estratti = dspy.OutputField(desc="Restituisci SOLO i codici esatti dei modelli separati da virgola (es. 'WX-111, YZ-222'). Nessun'altra parola.")

class TrovaColonneEfficienza(dspy.Signature):
    """Analizza le colonne del catalogo e trova le 3 colonne necessarie per i consumi elettrici."""
    colonne_disponibili = dspy.InputField(desc="Lista di tutte le colonne nel file CSV")
    colonna_potenza = dspy.OutputField(desc="Nome ESATTO della colonna che indica la Potenza Frigorifera Totale. Scrivi 'NESSUNA' se non c'è.")
    colonna_eer = dspy.OutputField(desc="Nome ESATTO della colonna per l'EER (Raffrescamento/Freddo). Scrivi 'NESSUNA' se non c'è.")
    colonna_cop = dspy.OutputField(desc="Nome ESATTO della colonna per il COP (Riscaldamento/Caldo). Scrivi 'NESSUNA' se non c'è.")
pulitore_modelli = dspy.Predict(EstraiListaModelli)
mappatore_energia = dspy.Predict(TrovaColonneEfficienza)

@tool
def calcola_consumo_elettrico(codici_modelli: str, kw_richiesti: float = 0.0) -> str:
    """Usa questo tool per calcolare il consumo elettrico (kW assorbiti) di uno o piu modelli.
    - codici_modelli: la frase o i codici da analizzare.
    - kw_richiesti: (opzionale) kW specificati dall'utente. Se non indicati, lascia 0.0."""
    
    print(f"\n[TOOL DSPy] Esecuzione CALCOLA_CONSUMO_ELETTRICO -> Modelli: {codici_modelli} | Input kW: {kw_richiesti}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    if not codici_modelli or codici_modelli.strip() == "":
        return "Dati incompleti. Chiedi all'utente il codice del modello."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    risultato_modelli = pulitore_modelli(testo_input=codici_modelli)
    stringa_pulita = risultato_modelli.modelli_estratti.strip()
    lista_codici = [c.strip() for c in stringa_pulita.split(",") if c.strip()]
    print(f"[DSPy] Modelli identificati: {lista_codici}")

    print("[DSPy] Ricerca colonne EER, COP e Potenza in corso...")
    risultato_colonne = mappatore_energia(colonne_disponibili=str(colonne_catalogo))
    
    col_potenza = risultato_colonne.colonna_potenza if risultato_colonne.colonna_potenza != 'NESSUNA' else None
    eer_col = risultato_colonne.colonna_eer if risultato_colonne.colonna_eer != 'NESSUNA' else None
    cop_col = risultato_colonne.colonna_cop if risultato_colonne.colonna_cop != 'NESSUNA' else None

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

def metrica_pulitore_modelli(example, pred, trace=None):
    """Giudica se Llama 3 ha risposto SOLO con i codici separati da virgola."""
    risposta = pred.modelli_estratti.lower()
    
    # Se l'IA si mette a chiacchierare, viene bocciata
    parole_vietate = ["ecco", "i modelli", "sono", "codici", "certamente"]
    for parola in parole_vietate:
        if parola in risposta:
            print(f"[METRICA Fallita] L'IA ha inserito testo inutile: '{parola}'")
            return False
    return True

def metrica_energia(example, pred, trace=None):
    colonne_scelte = [
        pred.colonna_potenza.strip(), 
        pred.colonna_eer.strip(), 
        pred.colonna_cop.strip()
    ]
    for col in colonne_scelte:
        if col != "NESSUNA" and col not in example.colonne_disponibili:
            print(f"[METRICA Fallita] Colonna inventata: '{col}'")
            return False
    return True

class TrovaColonnaPrevalenza(dspy.Signature):
    """Analizza le colonne del catalogo e trova la colonna esatta per la Prevalenza o Pressione dell'aria."""
    colonne_disponibili = dspy.InputField(desc="Lista di tutte le colonne nel file CSV")
    colonna_prevalenza = dspy.OutputField(desc="Nome ESATTO della colonna che indica la Prevalenza Massima (o Pressione Massima / Mandata). Scrivi 'NESSUNA' se non c'è.")
mappatore_prevalenza = dspy.Predict(TrovaColonnaPrevalenza)

@tool
def verifica_prevalenza_canali(codici_modelli: str, pascal_persi_impianto: float = 0.0) -> str:
    """Usa questo tool per verificare se la ventola del modello ha abbastanza prevalenza per superare la perdita di carico dei canali.
    - codici_modelli: la frase o i codici dei modelli separati da virgola.
    - pascal_persi_impianto: valore in Pascal (Pa) o perdita di carico richiesta."""
    
    print(f"\n[TOOL DSPy] Esecuzione VERIFICA_PREVALENZA -> Modelli: {codici_modelli} | Pascal: {pascal_persi_impianto}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    if not codici_modelli or codici_modelli.strip() == "":
        return "Dati incompleti. Chiedi all'utente il codice del modello."

    colonna_modello = trova_colonna_modello()
    if not colonna_modello:
        return "Errore: impossibile trovare la colonna del modello nel catalogo."

    risultato_modelli = pulitore_modelli(testo_input=codici_modelli)
    stringa_pulita = risultato_modelli.modelli_estratti.strip()
    lista_codici = [c.strip() for c in stringa_pulita.split(",") if c.strip()]
    print(f"[DSPy] Modelli identificati per prevalenza: {lista_codici}")

    print("[DSPy] Ricerca colonna Prevalenza/Pressione in corso...")
    risultato_colonna = mappatore_prevalenza(colonne_disponibili=str(colonne_catalogo))
    col_prevalenza = risultato_colonna.colonna_prevalenza.strip()
    
    print(f"[DSPy] Colonna trovata: {col_prevalenza}")

    if col_prevalenza == 'NESSUNA' or col_prevalenza not in colonne_catalogo:
        return "Errore: impossibile trovare una colonna relativa alla prevalenza o pressione nel catalogo."

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
def metrica_prevalenza(example, pred, trace=None):
    colonna_scelta = pred.colonna_prevalenza.strip()
    if colonna_scelta == "NESSUNA":
        return True
    if colonna_scelta in example.colonne_disponibili:
        return True
    print(f"[METRICA Fallita] L'IA ha inventato la colonna prevalenza: '{colonna_scelta}'")
    return False

@tool
def consulta_dizionario_catalogo(parola_chiave: str) -> str:
    """Usa questo tool per spiegare il significato di termini tecnici o acronimi (es. 'EER', 'prevalenza', 'portata').
    Non usarlo se l'utente ha gia fornito un codice modello specifico.
    - parola_chiave: il termine da cercare."""
    
    print(f"\n[TOOL DSPy] Esecuzione CONSULTA_DIZIONARIO -> Ricerca: '{parola_chiave}'")

    if not parola_chiave or parola_chiave.strip() == "":
        return "Errore: Devi fornire una parola chiave per cercare nel dizionario."

    # Imposta il percorso del file txt
    cartella_corrente = os.path.dirname(os.path.abspath(__file__))
    percorso_file = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

    try:
        with open(percorso_file, "r", encoding="utf-8") as f:
            contenuto = f.read()
    except FileNotFoundError:
        return "Errore: Il file del dizionario non è stato trovato."
    except UnicodeDecodeError:
        with open(percorso_file, "r", encoding="latin-1") as f:
            contenuto = f.read()

    # 1. RICERCA VELOCE CON PYTHON
    paragrafi = contenuto.split("\n\n")
    risultati = []
    parola = parola_chiave.strip().lower()

    for p in paragrafi:
        if parola in p.lower():
            risultati.append(p.strip())

    if not risultati:
        return f"Nessuna voce trovata per la parola chiave '{parola_chiave}'."

    # 2. ASSEMBLAGGIO DEL TESTO GREZZO TROVATO NEL TXT
    testo_grezzo = "\n\n".join(risultati[:5])
    
    # 3. CHIAMATA A DSPY (È qui che avviene la magia e viene generato il log!)
    print("[DSPy] Sintesi della spiegazione tecnica in corso...")
    
    try:
        # Passiamo le variabili alla Signature DSPy
        risultato_dspy = spiegatore_dizionario(
            parola_chiave=parola_chiave,
            testo_estratto=testo_grezzo
        )
        # Estraiamo la risposta finale pulita
        spiegazione_finale = risultato_dspy.spiegazione.strip()
        
    except Exception as e:
        print(f"[ERRORE DSPy] DSPy non è riuscito a generare la risposta: {e}")
        return testo_grezzo # Fallback: se DSPy fallisce, restituiamo il testo grezzo

    return spiegazione_finale

dati_visivi_temporanei = None

@tool
def prepara_dati_grafico(parametro_asse_y: str, tipo_visualizzazione: str = "grafico", top_n: int = 5) -> str:
    """Usa questo tool solo se l'utente scrive esplicitamente le parole 'grafico', 'diagramma' o 'tabella'.
    Per ricerche normali usa 'cerca_catalogo_generico'.
    - parametro_asse_y: nome esatto della colonna da analizzare.
    - tipo_visualizzazione: 'grafico' o 'tabella'.
    - top_n: numero di modelli da mostrare."""
    global dati_visivi_temporanei
    print(f"\n[TOOL] Esecuzione PREPARA_DATI_GRAFICO -> Asse Y: {parametro_asse_y} | Tipo: {tipo_visualizzazione}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    richiesta_esatta = parametro_asse_y.replace('_', ' ').strip().lower()
    colonna_reale = None
    
    for col in colonne_catalogo:
        if " ".join(col.split()).lower() == " ".join(richiesta_esatta.split()):
            colonna_reale = col
            break

    if not colonna_reale:
        return "Colonna non trovata. Chiedi all'utente di specificare meglio il parametro per il grafico."

    df = df_catalogo.copy()
    
    def pulisci_numero(valore):
        if isinstance(valore, str):
            return valore.replace('.', '').replace(',', '.')
        return valore
        
    df[colonna_reale] = df[colonna_reale].apply(pulisci_numero)
    df[colonna_reale] = pd.to_numeric(df[colonna_reale], errors="coerce")
    
    risultato = df.dropna(subset=[colonna_reale]).sort_values(by=colonna_reale, ascending=False).head(top_n)

    # L'interruttore che decide cosa passeremo al frontend
    tipo_scelto = "tabella" if tipo_visualizzazione == "tabella" else "grafico_barre"

    dati_per_grafico = {
        "tipo": tipo_scelto,
        "titolo": colonna_reale,
        "dati": []
    }

    for _, row in risultato.iterrows():
        nome_modello = str(row.get("Modello PAL", "Sconosciuto"))
        valore = float(row.get(colonna_reale, 0.0))
        dati_per_grafico["dati"].append({"Modello": nome_modello, "Valore": valore})

    dati_visivi_temporanei = dati_per_grafico

    return "Analisi visiva pronta."

class GeneraFiltroPandas(dspy.Signature):
    """Genera codice Python/Pandas perfetto per filtrare un dataframe in base alla richiesta dell'utente."""
    richiesta_utente = dspy.InputField(desc="La frase o le condizioni richieste dall'utente")
    colonne_disponibili = dspy.InputField(desc="Lista esatta delle colonne reali del dataframe chiamato 'df_lavoro'")
    codice_pandas = dspy.OutputField(desc="Scrivi SOLO il codice Python. Usa il dataframe 'df_lavoro' e salva il risultato ESATTAMENTE in una variabile chiamata 'dfrisultato'. Nessuna spiegazione, niente markdown, solo codice eseguibile.")
generatore_codice = dspy.Predict(GeneraFiltroPandas)


@tool 
def estrai_dati_dinamici(richiesta_utente: str) -> str:
    """Usa questo tool quando l'utente chiede di elaborare i dati in modo complesso o estrarre file dal catalogo."""
    global df_catalogo
    print(f"\n[TOOL DSPy] Esecuzione ESTRAI_DATI_DINAMICI")
    if df_catalogo is None: return "Errore: database catalogo non caricato."
         
    try:
        cartella_destinazione = os.path.dirname(CSV_GRAFICI_PATH)
        os.makedirs(cartella_destinazione, exist_ok=True)
        
        df_lavoro = df_catalogo.copy()
        df_lavoro.columns = df_lavoro.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
        
        risultato_dspy = generatore_codice(richiesta_utente=richiesta_utente, colonne_disponibili=str(list(df_lavoro.columns)))
        codice_pulito = risultato_dspy.codice_pandas.replace("```python", "").replace("```", "").strip()
        
        scatola_sicura = {"pd": pd, "df_lavoro": df_lavoro, "converti_serie_numerica": converti_serie_numerica, "dfrisultato": None}
        exec(codice_pulito, {}, scatola_sicura)

        df_finale = scatola_sicura.get("dfrisultato")
        if df_finale is not None and isinstance(df_finale, pd.DataFrame):
            df_finale.to_csv(CSV_GRAFICI_PATH, index=False)
            return "SUCCESSO: Dati estratti e salvati. ISTRUZIONE PER L'AI: Rispondi dicendo 'Ho estratto i dati nel file CSV con successo. Vuoi che ti generi anche un grafico?'."
        return "ERRORE: Generazione dataframe fallita."
    except Exception as e:
        return f"ERRORE DI ESECUZIONE PYTHON o DSPy: {e}"

def metrica_codice_pandas(example, pred, trace=None):
    codice = pred.codice_pandas
    parole_pericolose = ["import os", "import sys", "subprocess", "eval("]
    for parola in parole_pericolose:
        if parola in codice:
            print(f"[METRICA Fallita] Tentativo di codice pericoloso rilevato: {parola}")
            return False
    if "dfrisultato" not in codice:
        print("[METRICA Fallita] L'IA non ha assegnato il risultato alla variabile 'dfrisultato'.")
        return False
        
    if "df_lavoro" not in codice:
        print("[METRICA Fallita] L'IA non ha usato il dataframe di partenza 'df_lavoro'.")
        return False
    return True

class GeneraCodicePlotly(dspy.Signature):
    """Genera codice Python usando Plotly Express (px) per creare un grafico partendo dal dataframe 'df'."""
    richiesta_utente = dspy.InputField(desc="Che tipo di grafico vuole l'utente (es. a barre, a torta, scatterplot) e quali dati incrociare")
    colonne_disponibili = dspy.InputField(desc="I nomi reali delle colonne presenti nel dataframe")
    codice_plotly = dspy.OutputField(desc="Scrivi SOLO codice Python. Devi usare 'px' e 'df'. Devi creare e salvare il grafico ESATTAMENTE in una variabile chiamata 'fig'. Non usare .show(), non usare print. Nessuna spiegazione, solo il codice nudo e crudo.")
generatore_grafici = dspy.Predict(GeneraCodicePlotly)

@tool
def genera_grafico_avanzato(richiesta_utente: str) -> str:
    """Usa questo tool SOLO quando l'utente chiede esplicitamente un grafico partendo dal CSV generato."""
    global dati_visivi_temporanei
    print(f"\n[TOOL DSPy] Esecuzione GENERA_GRAFICO_AVANZATO")

    try:
        if not os.path.exists(CSV_GRAFICI_PATH): return "ERRORE: File dataframe_grafico.csv non trovato."
        df_temp = pd.read_csv(CSV_GRAFICI_PATH)
        if df_temp.empty: return "ERRORE: Il CSV è vuoto."

        df_temp.columns = df_temp.columns.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

        risultato_dspy = generatore_grafici(richiesta_utente=richiesta_utente, colonne_disponibili=str(list(df_temp.columns)))
        codice_pulito = risultato_dspy.codice_plotly.replace("```python", "").replace("```", "").strip()

        scatola_sicura = {"df": df_temp, "px": px, "pd": pd}
        exec(codice_pulito, {}, scatola_sicura)

        if "fig" not in scatola_sicura: return "ERRORE: Variabile 'fig' non creata dal codice."

        os.makedirs(DIR_GRAFICI_SALVATI, exist_ok=True)
        nome_file = f"grafico_{int(time.time())}.html"
        html_path = os.path.join(DIR_GRAFICI_SALVATI, nome_file)
        scatola_sicura["fig"].write_html(html_path, full_html=False, include_plotlyjs="cdn")
        
        # Salviamo il file nella "scatola segreta" e non nel testo!
        dati_visivi_temporanei = {"tipo": "grafico_html_file", "path": html_path}
        
        return "ISTRUZIONE PER L'AI: Il grafico è stato generato e salvato correttamente! L'interfaccia lo sta già mostrando. Tu DEVI rispondere ESATTAMENTE solo con questa frase: 'Ecco il grafico che hai richiesto!'. Non scusarti mai."
    except Exception as e:
        return f"ERRORE nella generazione del grafico: {e}"
    
def metrica_codice_plotly(example, pred, trace=None):
    codice = pred.codice_plotly
    parole_pericolose = ["import os", "import sys", "subprocess", "eval("]
    for parola in parole_pericolose:
        if parola in codice:
            print(f"[METRICA Fallita] Tentativo di codice pericoloso rilevato: {parola}")
            return False
    if "fig =" not in codice and "fig=" not in codice:
        print("[METRICA Fallita] L'IA non ha assegnato il grafico alla variabile 'fig'.")
        return False
    return True
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

    if chat_id not in memoria_conversazioni:
        istruzioni_di_sistema = SystemMessage(content="""Sei un assistente tecnico HVAC. Devi rispettare RIGOROSAMENTE questo albero decisionale (IF/THEN):

1. IF l'utente chiede un modello per CONDIZIONARE, RAFFRESCARE o RISCALDARE un ambiente:
- Controlla se hai: 1. Metri quadri, 2. Numero persone, 3. Temp. Esterna, 4. Temp. Interna.
- SE l'utente fa un follow-up, recupera i dati invariati dalla cronologia.
- SE CONTINUA A MANCARE UN DATO: Fermati e chiedilo.
- SE HAI TUTTI I DATI: Usa 'calcola_fabbisogno_termico' e poi 'cerca_catalogo_generico'.

2. IF l'utente chiede un modello per VENTILARE o garantire il RICAMBIO D'ARIA:
- Controlla se hai: 1. Metri quadri, 2. Numero persone, 3. Tipo di locale.
- SE l'utente sta aggiornando i numeri, recupera i dati invariati dalla chat.
- SE MANCA UN DATO: Fermati e chiedilo.
- SE HAI TUTTI I DATI: Usa 'calcola_portata_aria' e poi 'cerca_catalogo_generico'. Mostra SEMPRE almeno 3 modelli.

3. IF l'utente chiede il CONSUMO ELETTRICO o quale modello CONVIENE/CONSUMA MENO:
- Usa 'calcola_consumo_elettrico'. NON cercare l'efficienza prima, il tool la troverà da solo.

4. IF l'utente chiede la PREVALENZA o la COMPATIBILITÀ CON I CANALI (perdita di carico / Pascal):
- Usa 'verifica_prevalenza_canali' passando i modelli e i Pascal richiesti.

5. IF l'utente chiede un dato tecnico di un MODELLO SPECIFICO:
- Se nella richiesta è presente un codice modello esatto (es. 091-051), usa ESCLUSIVAMENTE 'cerca_catalogo_specifico'.
- Non usare 'consulta_dizionario_catalogo' se la domanda contiene il codice di un modello.

6. IF l'utente fa domande su manutenzione, filtri, installazione o "vapori/grassi":
- Usa ESCLUSIVAMENTE 'cerca_manuali' o 'cerca_sito_web'.

7. IF l'utente chiede spiegazioni tecniche, definizioni, o chiede se un parametro esiste nel catalogo (es. "rumorosità", "gas R32", "come viene misurato"):
- Usa 'consulta_dizionario_catalogo'. NON USARE tool matematici.

8. IF l'utente chiede un GRAFICO, un DIAGRAMMA o una TABELLA visiva:
- Usa il tool 'prepara_dati_grafico' SOLO E SOLTANTO SE l'utente ha scritto testualmente una di queste tre parole.
- Per le ricerche normali usa sempre 'cerca_catalogo_generico'.

9. IF l'utente chiede estrazioni dati particolari, incroci complessi, o usa parole come "crea un nuovo file", "estrai i dati per Python":
- Usa il tool 'estrai_dati_dinamici'. Passagli la richiesta completa dell'utente.

10. IF l'utente chiede di creare un GRAFICO o PLOTTARE i dati che sono stati estratti nel CSV:
- Usa il tool 'genera_grafico_avanzato' passando la frase intera dell'utente.
- Usa questo tool SOLO se esistono già dati estratti nel CSV.
- Non descrivere il grafico a parole se puoi generarlo davvero.

REGOLE GLOBALI:
- Rispondi SOLO in Italiano.
- NON inventare parametri. Se non li sai, chiedili.
- DIVIETO DI CALCOLO A VUOTO: Se l'utente fa una domanda puramente discorsiva e NON fornisce numeri (kW, mq, persone, Pascal), ti è ASSOLUTAMENTE VIETATO usare i tool di calcolo (termico, aria, elettrico, prevalenza). Usa solo il dizionario o rispondi a parole.
- DIVIETO DI JSON: Non rispondere mai mostrando codice JSON grezzo all'utente.
- DIVIETO CHIAMATE MULTIPLE: Chiama UN SOLO tool alla volta, attendi il risultato, poi rispondi.
- REGOLA ANTI-LOOP: Dopo aver ricevuto i dati da qualsiasi tool, formula IMMEDIATAMENTE la risposta discorsiva per l'utente e fermati. Non richiamare lo stesso tool o altri tool per verifiche extra.
- REGOLA DI PRIVACY: Non menzionare mai nomi di cartelle, percorsi di file o dettagli del sistema operativo nelle tue risposte.""")
        memoria_conversazioni[chat_id] = [istruzioni_di_sistema]

    memoria_conversazioni[chat_id].append(HumanMessage(content=user_query))

    match_modello_specifico = re.search(r"\b\d{3}-\d{3}\b", user_query)
    richiesta_visiva = any(parola in user_query.lower() for parola in ["grafico", "diagramma", "tabella", "analisi visiva"])

    messaggi_completi = memoria_conversazioni[chat_id]
    messaggi_per_llm = [messaggi_completi[0]]

    if len(messaggi_completi) > 7:
        messaggi_per_llm.extend(messaggi_completi[-6:])
    else:
        messaggi_per_llm.extend(messaggi_completi[1:])

    current_state = {"messages": messaggi_per_llm}

    try:
        # attiva il cronometro prima che l'llm inizi a pensare
        start_time = time.time()
        result = app.invoke(current_state, {"recursion_limit": 10})
        end_time = time.time()
        tempo_trascorso = end_time - start_time
        print(f"\n[DEBUG TEMPO] Tempo di risposta: {tempo_trascorso:.2f} secondi")
    except Exception as e:
        return {"testo": f"Si è verificato un errore nel motore: {e}", "azioni": []}

    nuovi_messaggi = result["messages"]

    risposta_assistente = ""
    for msg in reversed(nuovi_messaggi):
        if hasattr(msg, "content") and isinstance(msg.content, str) and msg.content.strip() != "":
            risposta_assistente = msg.content
            break

    memoria_conversazioni[chat_id].append(AIMessage(content=risposta_assistente))

    tool_usati = []
    for msg in nuovi_messaggi:
        if hasattr(msg, 'tool_calls') and msg.tool_calls:
            for tool in msg.tool_calls:
                if tool['name'] not in tool_usati:
                    tool_usati.append(tool['name'])

    global dati_visivi_temporanei
    
    if dati_visivi_temporanei is not None:
        risposta_assistente = "Ecco il grafico che hai richiesto basato sui dati estratti!"
    else:
        prompt_finale = f"""
        Sei un assistente tecnico HVAC. Riscrivi in italiano naturale la risposta finale per l'utente.
        REGOLE OBBLIGATORIE:
        - Non mostrare mai istruzioni interne, nomi di tool o variabili JSON.
        - Se ci sono modelli, presentali in un elenco puntato chiaro.
        - Mantieni solo le informazioni utili all'utente finale.
        - Se il risultato dice 'Ho estratto i dati nel file CSV', confermalo con entusiasmo.

        Domanda utente: {user_query}
        Risultato grezzo: {risposta_grezza}
        """
        try:
            risposta_assistente = llm.invoke([HumanMessage(content=prompt_finale)]).content.strip()
        except Exception:
            risposta_assistente = pulisci_risposta_tool_per_utente(risposta_grezza)

    memoria_conversazioni[chat_id].append(AIMessage(content=risposta_assistente))
    dati_da_esportare = dati_visivi_temporanei
    dati_visivi_temporanei = None

    salva_prompt_dspy()

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
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    temperature=0,
    groq_api_key=GROQ_API_KEY
)
llm_con_tools = llm.bind_tools(tools)

tool_node = ToolNode(tools)

workflow = StateGraph(AgentState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

def route_after_tool(state: AgentState) -> str:
    global dati_visivi_temporanei
    if dati_visivi_temporanei is not None:
        return "end"
    return "agent"

workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
workflow.add_conditional_edges("tools", route_after_tool, {"agent": "agent", "end": END})

app = workflow.compile()

def salva_prompt_dspy():
    cartella_destinazione = r"C:\Users\PC_A87\Desktop\Carricamento Progetti GIT\pipeline\data\2-processing"
    
    if not os.path.exists(cartella_destinazione):
        os.makedirs(cartella_destinazione, exist_ok=True)
    
    percorso_file = os.path.join(cartella_destinazione, "log_prompts_dspy.txt")
    
    lm = dspy.settings.lm
    if not lm or not hasattr(lm, 'history') or len(lm.history) == 0:
        return

    with open(percorso_file, "a", encoding="utf-8") as f:
        f.write("\n\n" + "#"*70 + "\n")
        f.write(f" NUOVA SESSIONE DI TEST - DATA E ORA: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("#"*70 + "\n\n")
        for i, record in enumerate(lm.history):
            f.write(f"--- CHIAMATA API #{i+1} ---\n")
            prompt_testo = ""
            if 'prompt' in record and record['prompt']:
                prompt_testo = record['prompt']
            elif 'messages' in record and record['messages']:
                if isinstance(record['messages'], list):
                    for msg in record['messages']:
                        ruolo = msg.get('role', 'Sconosciuto').upper()
                        contenuto = msg.get('content', '')
                        prompt_testo += f"[{ruolo}]:\n{contenuto}\n\n"
                else:
                    prompt_testo = str(record['messages'])
            elif 'kwargs' in record and 'messages' in record['kwargs']:
                prompt_testo = str(record['kwargs']['messages'])
            else:
                prompt_testo = f"FORMATO SCONOSCIUTO:\n{str(record)}" 
            f.write(f"{prompt_testo}\n")
            f.write("\n--- RISPOSTA GENERATA DA DSPY ---\n")
            risposta = record.get('response', 'Nessuna risposta trovata')
            if isinstance(risposta, list):
                risposta = "\n".join([str(r) for r in risposta])
            f.write(f"{risposta}\n")
            f.write("-" * 70 + "\n")
    lm.history.clear()
    print(f"Log aggiornato con successo in: {percorso_file}")