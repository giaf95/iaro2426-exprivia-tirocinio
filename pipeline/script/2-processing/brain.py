import os
import sqlite3
import time
import pandas as pd
import difflib
import re
import time
from typing import Annotated, List, TypedDict
import operator
from langgraph.graph import StateGraph, END
from langchain_ollama import ChatOllama
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
import plotly.express as px

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
def cerca_catalogo_specifico(modello: str, parametro: str = "Tutti") -> str:
    """Usa questo tool SEMPRE e SOLO quando l'utente nomina un MODELLO SPECIFICO (es. '091-051' o '061-035').
    ARGOMENTI DA PASSARE DIRETTAMENTE:
    - modello: estrai SOLO il codice esatto (es. '091-051').
    - parametro: la grandezza fisica da cercare (es. 'Portata Massima Mandata')."""
    print(f"\n[TOOL] Esecuzione CERCA_CATALOGO_SPECIFICO")
    print(f"[TOOL] Ricerca chirurgica -> Modello: '{modello}' | Parametro: '{parametro}'")
    
    if df_catalogo is None:
        return "Errore: file Excel non caricato."
    
    # pulizia del codice cercato
    codice_pulito = modello.upper().replace("MODELLO", "").strip()
    
    # cerca la riga esatta nel DataFrame Pandas
    df_modello = df_catalogo[df_catalogo['Modello PAL'].astype(str).str.upper().str.contains(codice_pulito, na=False)]
    
    if df_modello.empty:
        return f"Modello {codice_pulito} non trovato nel catalogo Excel."
        
    # salvagente anti-crash se l'AI dimentica il parametro
    if parametro == "Tutti" or parametro.strip() == "":
         return f"Hai trovato il modello {codice_pulito}, ma non hai estratto il parametro richiesto! Chiedi all'utente cosa vuole sapere di preciso (es. dimensioni, peso, portata)."
         
    # cerca le colonne che contengono la parola richiesta
    richiesta_pulita = parametro.lower().strip()
    colonne_trovate = [col for col in colonne_catalogo if richiesta_pulita in str(col).lower()]
    
    if not colonne_trovate:
         return f"Il parametro '{parametro}' non esiste nel catalogo. Dì all'utente di specificare meglio la parola chiave."
         
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
def calcola_fabbisogno_termico(area_mq: float, numero_persone: int, temp_esterna: float, temp_interna: float, tipo_locale: str) -> str:
    """Usa questo tool per calcolare i kW necessari per condizionare una stanza.
    DIVIETO ASSOLUTO: NON INVENTARE LE TEMPERATURE. Se l'utente non ti ha scritto esplicitamente quanti gradi ci sono fuori e quanti ne vuole dentro, FERMATI E CHIEDILI.
    PARAMETRI:
    - area_mq: metri quadri della stanza.
    - numero_persone: quante persone occupano la stanza.
    - temp_esterna: temperatura in gradi all'esterno (es. 35).
    - temp_interna: temperatura desiderata all'interno (es. 22).
    - tipo_locale: es. 'discoteca', 'ufficio', 'ristorante', ecc."""
    print(f"\n[TOOL] Esecuzione CALCOLA_FABBISOGNO_TERMICO")
    
    # faccio calcolare il Delta T a Python, non all'AI
    delta_t = abs(temp_esterna - temp_interna)
    print(f"[TOOL] Dati: {area_mq}mq, {numero_persone} persone, T.Est: {temp_esterna}°, T.Int: {temp_interna}° (Delta: {delta_t}°), locale: {tipo_locale}")

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

    return f"Calcolo completato: {fabbisogno_kw:.2f} kW. INSTRUZIONE PER L'AI: Ora usa il tool 'cerca_catalogo_generico'. Inserisci come parametro_richiesto ESATTAMENTE 'Potenza frigorifera totale macchina' e inserisci {fabbisogno_kw:.2f} nel campo 'valore_target'."

@tool
def calcola_portata_aria(area_mq: float, numero_persone: int, tipo_locale: str = "") -> str:
    """Usa questo tool per calcolare il fabbisogno di ventilazione (m3/h).
    REGOLA ANTI-INVENZIONE: Se l'utente NON ti ha scritto i numeri esatti nel messaggio, DEVI passare 0 (zero). Se non sai il tipo di locale, non inventarlo e lascia la stringa vuota "".
    PARAMETRI:
    - area_mq: metri quadri (inserisci 0 se non forniti).
    - numero_persone: quantità di persone (inserisci 0 se non fornite).
    - tipo_locale: es. 'scuola', 'palestra', 'ufficio'. (lascia "" se non lo sai)."""
    print(f"\n[TOOL] Esecuzione CALCOLA_PORTATA_ARIA")
    
    #guardrail (Numeri)
    if area_mq <= 0 or numero_persone <= 0:
        print("[TOOL] Dati numerici mancanti rilevati. Blocco dell'esecuzione.")
        return "ISTRUZIONE PER L'AI: Dati incompleti. FERMATI e NON usare il catalogo generico. Rispondi all'utente chiedendo di fornirti i metri quadri e il numero di persone."

    #guardrail (Testo / Amnesia)
    if not tipo_locale or tipo_locale.strip() == "":
         tipo_locale = "generico"

    # 1. Calcolo basato sulle persone (Fabbisogno per persona)
    m3h_persona = 40 # Standard uffici/residenziale
    tipo_locale_low = tipo_locale.lower()
    if "palestra" in tipo_locale_low or "discoteca" in tipo_locale_low or "ristorante" in tipo_locale_low:
        m3h_persona = 60
        
    fabbisogno_persone = numero_persone * m3h_persona
    
    # 2. Calcolo basato sui ricambi d'aria del volume (ACH)
    altezza_media = 3.0 # Assumiamo 3 metri di altezza standard
    volume = area_mq * altezza_media
    
    ach = 2.0 # Ricambi/ora standard
    if "scuola" in tipo_locale_low or "ristorante" in tipo_locale_low:
        ach = 4.0
    elif "palestra" in tipo_locale_low or "discoteca" in tipo_locale_low:
        ach = 6.0
        
    fabbisogno_volumetrico = volume * ach
    
    # Prendi il valore più alto
    portata_finale = max(fabbisogno_persone, fabbisogno_volumetrico)
    
    print(f"[TOOL] MAX tra Persone ({fabbisogno_persone}) e Volume ({fabbisogno_volumetrico}) = {portata_finale} m3/h")

    return f"Calcolo completato: {portata_finale:.2f} m3/h. INSTRUZIONE PER L'AI: Ora usa il tool 'cerca_catalogo_generico'. Parametro: 'Portata Massima Mandata Standard'. Target: {portata_finale:.2f}."

@tool
def calcola_consumo_elettrico(codici_modelli: str, kw_richiesti: float = 0.0) -> str:
    """Usa questo tool per calcolare il consumo elettrico (kW assorbiti) di uno o più modelli.
    PARAMETRI:
    - codici_modelli: i codici dei modelli da analizzare. Se l'utente chiede un confronto tra più modelli, inseriscili tutti separati da virgola (es. 'modelloA, modelloB').
    - kw_richiesti: (Opzionale) Estrai il numero di kW dal messaggio dell'utente. Se non specificato, lascia 0.0."""
    print(f"\n[TOOL] Esecuzione CALCOLA_CONSUMO_ELETTRICO -> Modelli: {codici_modelli} | Input kW: {kw_richiesti}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    if not codici_modelli or codici_modelli.strip() == "":
        return "Dati incompleti. Chiedi all'utente il codice del modello."

    # 1. pulisce la stringa dell'AI e la divide in una lista (gestendo "e", "o", "oppure", virgole)
    stringa_pulita = codici_modelli.lower().replace(' e ', ',').replace(' o ', ',').replace(' oppure ', ',')
    lista_codici = [c.strip() for c in stringa_pulita.split(',') if c.strip()]

    risultati_finali = []

    # 2.ciclo for: calcola il consumo per ogni modello richiesto
    for codice_singolo in lista_codici:
        codice_pulito = codice_singolo.upper().replace("MODELLO", "").strip()
        df_modello = df_catalogo[df_catalogo['Modello PAL'].astype(str).str.upper().str.contains(codice_pulito, na=False)]

        if df_modello.empty:
            risultati_finali.append(f"Modello {codice_pulito} non trovato nel catalogo.")
            continue

        risultati = []
        kw_attuali = kw_richiesti

        # auto-recupero
        if kw_attuali <= 0:
            col_potenza = [col for col in colonne_catalogo if 'potenza frigorifera totale' in str(col).lower()]
            if col_potenza:
                val_potenza = df_modello.iloc[0].get(col_potenza[0], 0)
                try:
                    kw_attuali = float(str(val_potenza).replace(',', '.'))
                    risultati.append(f"*(Nota: calcolo basato su potenza di targa di {kw_attuali} kW)*")
                except:
                    risultati_finali.append(f"Modello {codice_pulito}: Impossibile determinare i kW.")
                    continue
            else:
                 risultati_finali.append(f"Modello {codice_pulito}: Impossibile determinare i kW.")
                 continue

        # estrazione di EER e COP
        eer_col = [col for col in colonne_catalogo if 'eer' in str(col).lower()]
        cop_col = [col for col in colonne_catalogo if 'cop' in str(col).lower()]

        if eer_col:
            valore_eer = df_modello.iloc[0].get(eer_col[0], "N/D")
            try:
                eer_float = float(str(valore_eer).replace(',', '.'))
                consumo_freddo = kw_attuali / eer_float
                risultati.append(f"- Raffrescamento (EER {eer_float}): assorbe circa {consumo_freddo:.2f} kW elettrici.")
            except:
                pass

        if cop_col:
            valore_cop = df_modello.iloc[0].get(cop_col[0], "N/D")
            try:
                cop_float = float(str(valore_cop).replace(',', '.'))
                consumo_caldo = kw_attuali / cop_float
                risultati.append(f"- Riscaldamento (COP {cop_float}): assorbe circa {consumo_caldo:.2f} kW elettrici.")
            except:
                pass

        if risultati:
            risultati_finali.append(f"**Modello {codice_pulito}** (Carico: {kw_attuali} kW):\n" + "\n".join(risultati))
        else:
            risultati_finali.append(f"Modello {codice_pulito}: dati di efficienza validi non trovati.")

@tool
def verifica_prevalenza_canali(codici_modelli: str, prevalenza_richiesta_pa: float = 0.0) -> str:
    """Usa questo tool per verificare se i modelli hanno abbastanza prevalenza per i canali dell'aria.
    PARAMETRI:
    - codici_modelli: i codici dei modelli separati da virgola (es. '061-035, 091-051').
    - prevalenza_richiesta_pa: la perdita di carico in Pascal (Pa) dell'impianto. Se non specificata, lascia 0.0."""
    print(f"\n[TOOL] Esecuzione VERIFICA_PREVALENZA -> Modelli: {codici_modelli} | Pascal: {prevalenza_richiesta_pa}")

    if df_catalogo is None:
        return "Errore: database catalogo non caricato."

    if not codici_modelli or codici_modelli.strip() == "":
        return "Dati incompleti. Chiedi all'utente il codice del modello."

    # pulisce la stringa base
    stringa_pulita = codici_modelli.lower().replace(' e ', ',').replace(' o ', ',').replace(' oppure ', ',')
    
    # divide i modelli senza usare list comprehension
    parti = stringa_pulita.split(',')
    lista_codici = []
    for p in parti:
        if p.strip() != "":
            lista_codici.append(p.strip())

    risultati_finali = []

    # controlla ogni singolo modello
    for codice_singolo in lista_codici:
        codice_pulito = codice_singolo.upper().replace("MODELLO", "").strip()
        df_modello = df_catalogo[df_catalogo['Modello PAL'].astype(str).str.upper().str.contains(codice_pulito, na=False)]

        if df_modello.empty:
            risultati_finali.append(f"Modello {codice_pulito} non trovato.")
            continue

        # cerca la colonna della prevalenza
        col_prevalenza = ""
        for col in colonne_catalogo:
            if 'prevalenza massima mandata' in str(col).lower():
                col_prevalenza = col
                break

        if col_prevalenza == "":
            risultati_finali.append(f"Modello {codice_pulito}: impossibile trovare i dati di prevalenza.")
            continue

        valore_prev = df_modello.iloc[0].get(col_prevalenza, 0)
        
        try:
            prev_float = float(str(valore_prev).replace(',', '.'))
        except:
            prev_float = 0.0

        # confronta i valori
        if prevalenza_richiesta_pa <= 0:
            risultati_finali.append(f"Modello {codice_pulito}: ha una prevalenza massima di {prev_float} Pa (richiesta non specificata).")
        else:
            if prev_float >= prevalenza_richiesta_pa:
                risultati_finali.append(f"**Modello {codice_pulito}: COMPATIBILE.** Ha {prev_float} Pa, superiore ai {prevalenza_richiesta_pa} Pa richiesti.")
            else:
                risultati_finali.append(f"**Modello {codice_pulito}: NON COMPATIBILE.** Ha solo {prev_float} Pa, insufficiente per i {prevalenza_richiesta_pa} Pa richiesti.")

    # unisce i risultati
    testo_ritorno = ""
    for r in risultati_finali:
        testo_ritorno = testo_ritorno + r + "\n\n"
    
    return testo_ritorno.strip()

@tool
def consulta_dizionario_catalogo(parola_chiave: str) -> str:
    """Usa questo tool SOLO per spiegare il SIGNIFICATO di termini tecnici o acronimi (es. 'EER', 'prevalenza').
    DIVIETO ASSOLUTO: VIETATO usare questo tool se l'utente nomina un MODELLO SPECIFICO (es. '091-051'). Per i modelli usa 'cerca_catalogo_specifico'.
    ARGOMENTI DA PASSARE DIRETTAMENTE:
    - parola_chiave: la parola da cercare. OBBLIGATORIO."""
    print(f"\n[TOOL] Esecuzione CONSULTA_DIZIONARIO -> Ricerca: '{parola_chiave}'")

    # Percorso relativo sicuro
    cartella_corrente = os.path.dirname(os.path.abspath(__file__))
    percorso_file = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

    try:
        with open(percorso_file, 'r', encoding='utf-8') as f:
            contenuto = f.read()
    except FileNotFoundError:
        return "Errore: Il file 'dizionario_catalogo.txt' non è stato trovato. Avvisa l'utente."

    if parola_chiave and parola_chiave.strip() != "":
        paragrafi = contenuto.split('\n\n')
        risultati = []
        
        # ciclo for classico
        for p in paragrafi:
            if parola_chiave.lower() in p.lower():
                risultati.append(p)

        if len(risultati) > 0:
            testo_ritorno = f"DATI ESTRATTI PER '{parola_chiave}':\n"
            for r in risultati:
                testo_ritorno = testo_ritorno + r + "\n\n"
            
            return testo_ritorno.strip()
        else:
            return f"Nessuna voce trovata per la parola chiave '{parola_chiave}'."

    return "Errore: Devi fornire una parola chiave per cercare nel dizionario."

dati_visivi_temporanei = None

@tool
def prepara_dati_grafico(parametro_asse_y: str, tipo_visualizzazione: str = "grafico", top_n: int = 5) -> str:
    """Usa questo tool SOLO SE l'utente scrive ESPLICITAMENTE le parole 'grafico', 'diagramma' o 'tabella'.
    DIVIETO ASSOLUTO: Se l'utente fa una ricerca normale (es. 'quali macchine...', 'mostrami i modelli...'), NON ESSERE PROATTIVO per abbellire la risposta: ti è VIETATO usare questo tool. Usa 'cerca_catalogo_generico'.
    ARGOMENTI DA PASSARE DIRETTAMENTE:
    - parametro_asse_y: Inserisci ESATTAMENTE il nome della colonna da analizzare.
    - tipo_visualizzazione: Scrivi "grafico" se l'utente chiede un grafico/diagramma. Scrivi "tabella" se chiede una tabella.
    - top_n: il numero di modelli da mostrare."""
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

@tool 
def estrai_dati_dinamici(richiesta_utente: str) -> str:
    """Usa questo tool ESCLUSIVAMENTE quando l'utente chiede di elaborare i dati in modo complesso, estrarre file o creare nuovi dataframe personalizzati. 
    Passa in 'richiesta_utente' la frase esatta dell'utente."""
    global df_catalogo, llm # Richiamiamo le variabili globali già caricate
    
    if df_catalogo is None:
         return "Errore: database catalogo non caricato in memoria."
         
    try:
        cartella_corrente = os.path.dirname(os.path.abspath(__file__))
        cartella_pipeline = os.path.dirname(os.path.dirname(cartella_corrente))
        cartella_temp = os.path.join(cartella_pipeline, 'data', '1-preprocessing')
        os.makedirs(cartella_temp, exist_ok=True)
        path_salvataggio = os.path.join(cartella_temp, 'dataframe_grafico.csv')
        
        colonne_reali = list(df_catalogo.columns)
        prompt = f"""
        Sei un programmatore Python. Scrivi uno script Pandas per soddisfare questa richiesta: '{richiesta_utente}'
        
        Hai a disposizione il dataframe 'df_catalogo' (già caricato in memoria). 
        Colonne disponibili (usa ESATTAMENTE questi nomi per evitare KeyError): {colonne_reali}
        
        REGOLE:
        1. Filtra 'df_catalogo' e salva il risultato in una variabile chiamata ESATTAMENTE 'df_risultato'.
        2. NON importare pandas, NON usare read_csv.
        3. Restituisci SOLO il codice dentro i backtick ```python ... ```. Non aggiungere spiegazioni o commenti testuali.
        """
        
        # Usiamo l'LLM globale, senza instanziarlo di nuovo
        risposta_llm = llm.invoke(prompt)
        testo_risposta = risposta_llm.content
        
        # Estrazione sicura tramite regex
        match = re.search(r"```python\n(.*?)\n```", testo_risposta, re.DOTALL)
        if match:
            codice_pulito = match.group(1).strip()
        else:
            codice_pulito = testo_risposta.replace("```python", "").replace("```", "").strip()
            
        print(f"\n[DEBUG LLM] Codice Pandas generato e in esecuzione:\n{codice_pulito}\n")
    
        scatola_sicura = {
            "pd": pd,
            "df_catalogo": df_catalogo.copy() # Lavoriamo su una copia per non sporcare l'originale
        }
        
        # Avviso di sicurezza: exec() esegue codice arbitrario. In produzione è un rischio.
        exec(codice_pulito, {}, scatola_sicura)
        df_finale = scatola_sicura.get("df_risultato")
        
        if df_finale is None:
            return "Errore: il codice generato non ha prodotto una variabile 'df_risultato'."
        
        df_finale.to_csv(path_salvataggio, index=False)
        return f"Dati dinamici estratti ed elaborati con successo. Avvisa l'utente."
    except Exception as e:
        return f"ERRORE DI ESECUZIONE PYTHON: {e}\n\n=== STOP TOOL ===\nORDINE PER L'AI: Il codice ha generato un errore. NON RITENTARE. Rispondi all'utente dicendo che non sei riuscito a generare il codice corretto per l'estrazione."

@tool
def genera_grafico_avanzato(richiesta_utente: str) -> str:
    """Usa questo tool ESCLUSIVAMENTE quando l'utente ti chiede di disegnare o generare un grafico complesso basato sull'ultimo file CSV estratto.
    Passa l'intera richiesta dell'utente nel parametro 'richiesta_utente'."""
    global llm, dati_visivi_temporanei
    print(f"\n[TOOL] Esecuzione GENERA_GRAFICO_AVANZATO -> Richiesta: '{richiesta_utente}'")
    
    try:
        # Costruzione sicura dei percorsi con os.path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pipeline_dir = os.path.dirname(os.path.dirname(script_dir))
        
        csv_path = os.path.join(pipeline_dir, 'data', '1-preprocessing', 'dataframe_grafico.csv')
        cartella_ui = os.path.join(pipeline_dir, 'data', '3-user_interface')
        os.makedirs(cartella_ui, exist_ok=True)
        json_path = os.path.join(cartella_ui, 'grafico_generato.json')
        
        if not os.path.exists(csv_path):
            return "Errore: Il file 'dataframe_grafico.csv' non esiste. Dì all'utente che deve prima estrarre i dati."
            
        # Leggiamo le colonne dal CSV per dare contesto all'LLM e non fargliele inventare
        df_temp = pd.read_csv(csv_path)
        colonne = list(df_temp.columns)
        
        prompt = f"""
        Sei un programmatore Python esperto in data visualization.
        Il tuo compito è scrivere il codice per generare un grafico Plotly basato su questa richiesta: '{richiesta_utente}'
        
        Hai a disposizione due variabili testuali già pronte: 
        - 'csv_path': il percorso del file CSV da leggere.
        - 'json_path': il percorso dove salvare il grafico.
        Hai anche le librerie 'pd' (pandas) e 'px' (plotly.express) GIA' INCLUSE E PRONTE. NON importarle.
        
        Ecco le colonne presenti nel CSV: {colonne}
        
        REGOLE:
        1. Carica i dati in questo modo: df = pd.read_csv(csv_path)
        2. Genera la figura con px e salvala in una variabile chiamata ESATTAMENTE 'fig'.
        3. Salva la figura sul disco in questo modo esatto: fig.write_json(json_path)
        4. DIVIETO ASSOLUTO: NON USARE fig.show().
        5. Restituisci SOLO il codice dentro i backtick ```python ... ``` senza chiacchiere.
        """
        
        # Usiamo l'istanza globale di Qwen senza ricaricarlo in RAM
        risposta_llm = llm.invoke(prompt)
        testo_risposta = risposta_llm.content
        
        # Estrazione sicura del codice
        match = re.search(r"```python\n(.*?)\n```", testo_risposta, re.DOTALL)
        if match:
            codice_pulito = match.group(1).strip()
        else:
            codice_pulito = testo_risposta.replace("```python", "").replace("```", "").strip()
            
        print(f"\n[DEBUG LLM] Codice Plotly generato:\n{codice_pulito}\n")
        
        # Esegue il codice passando i path come stringhe nell'ambiente per farglieli usare
        scatola_sicura = {
            "csv_path": csv_path,
            "json_path": json_path,
            "pd": pd,
            "px": px
        }
        
        exec(codice_pulito, {}, scatola_sicura)
        
        if not os.path.exists(json_path):
             return "Errore: Il codice è stato eseguito ma il file JSON del grafico non è stato creato sull'hard disk."
             
        # Agganciamo il segnale per avvisare Streamlit che c'è un nuovo file da disegnare
        dati_visivi_temporanei = {
             "tipo": "grafico_json",
             "path": json_path
        }
        
        return "Ho analizzato i dati e preparato il grafico. Eccolo qui sotto!"
        
    except Exception as e:
        return f"ERRORE DI ESECUZIONE PYTHON: {e}\n\n=== STOP TOOL ===\nORDINE PER L'AI: Il codice Plotly ha fallito. NON RITENTARE per evitare loop. Avvisa l'utente."

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
- È ASSOLUTAMENTE VIETATO usare 'consulta_dizionario_catalogo' se la domanda contiene il codice di un modello.

6. IF l'utente fa domande su manutenzione, filtri, installazione o "vapori/grassi":
- Usa ESCLUSIVAMENTE 'cerca_manuali' o 'cerca_sito_web'.

7. IF l'utente chiede spiegazioni tecniche, definizioni, o chiede se un parametro esiste nel catalogo (es. "rumorosità", "gas R32", "come viene misurato"):
- Usa 'consulta_dizionario_catalogo'. NON USARE tool matematici.

8. IF l'utente chiede un GRAFICO, un DIAGRAMMA o una TABELLA visiva:
- Usa il tool 'prepara_dati_grafico' SOLO E SOLTANTO SE l'utente ha scritto testualmente una di queste tre parole.
- NON usare questo tool di tua iniziativa per "abbellire" i risultati. Per le ricerche normali sei OBBLIGATO a usare sempre 'cerca_catalogo_generico'.

9. IF l'utente chiede estrazioni dati particolari, incroci complessi, o usa parole come "crea un nuovo file", "estrai i dati per Python":
- Usa il tool 'estrai_dati_dinamici'. Passagli la richiesta completa dell'utente in modo che possa generare il codice corretto.

10. IF l'utente chiede di creare un GRAFICO, un DIAGRAMMA o PLOTTARE i dati che sono appena stati estratti o filtrati nel CSV:
- Usa il tool 'genera_grafico_avanzato' passando la frase intera dell'utente.

REGOLE GLOBALI:
- Rispondi SOLO in Italiano.
- NON inventare parametri. Se non li sai, chiedili.
- DIVIETO DI CALCOLO A VUOTO: Se l'utente fa una domanda puramente discorsiva e NON fornisce numeri (kW, mq, persone, Pascal), ti è ASSOLUTAMENTE VIETATO usare i tool di calcolo (termico, aria, elettrico, prevalenza). Usa solo il dizionario o rispondi a parole.
- DIVIETO DI JSON: È severamente vietato rispondere mostrando codice JSON grezzo all'utente.
- DIVIETO CHIAMATE MULTIPLE: Ti è ASSOLUTAMENTE VIETATO chiamare due tool contemporaneamente. Scegli UN SOLO tool alla volta, attendi il risultato, e poi rispondi all'utente.
- REGOLA ANTI-LOOP: Dopo aver ricevuto i dati da QUALSIASI tool, ti è ASSOLUTAMENTE VIETATO richiamare lo stesso tool o chiamarne altri per fare verifiche extra. Devi IMMEDIATAMENTE formulare la risposta discorsiva per l'utente, basandoti sui dati estratti, e fermarti.""")
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
    dati_da_esportare = dati_visivi_temporanei
    dati_visivi_temporanei = None

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

tools = [#cerca_catalogo_specifico, 
         #cerca_catalogo_generico, 
         #cerca_sito_web, cerca_manuali, 
         #calcola_fabbisogno_termico, 
         #calcola_portata_aria, 
         #calcola_consumo_elettrico, 
         #verifica_prevalenza_canali,
         #consulta_dizionario_catalogo,
         #prepara_dati_grafico,
         estrai_dati_dinamici,
         genera_grafico_avanzato]

# configurazione LangGraph e LLM
# parametri aggiunti per limitare i consumi della cpu e della ram
llm = ChatOllama(model="gemma3:4b", temperature=0, num_thread=4, num_ctx=1536)
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