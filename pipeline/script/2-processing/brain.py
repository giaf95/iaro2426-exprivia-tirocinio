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
from langchain_core.messages import HumanMessage, SystemMessage
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

    # blocco di sicurezza anti-loop
    testo_ritorno = testo_ritorno + "\n\n=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: Il calcolo termico in kW è completato! ORA FERMATI. NON chiamare nessun altro tool. Scrivi immediatamente la risposta finale all'utente riportando i risultati."
    
    return testo_ritorno

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

    # blocco di sicurezza anti-loop
    testo_ritorno = testo_ritorno + "\n\n=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: Il calcolo della portata d'aria in m3/h è completato! ORA FERMATI. NON chiamare nessun altro tool. Scrivi immediatamente la risposta finale all'utente riportando i risultati."
    
    return testo_ritorno

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

        # blocco di sicurezza anti-loop
    testo_ritorno = "\n\n".join(risultati_finali)
    testo_ritorno = testo_ritorno + "\n\n=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: Il calcolo dei consumi elettrici è completato! ORA FERMATI. NON chiamare nessun altro tool. Scrivi immediatamente la risposta finale all'utente."
    
    return testo_ritorno

@tool
def verifica_prevalenza_canali(codici_modelli: str, pascal_persi_impianto: float = 0.0) -> str:
    """Usa questo tool per verificare se la ventola del modello ha abbastanza forza (prevalenza) per superare la perdita di carico dei canali dell'utente.
    PARAMETRI:
    - codici_modelli: i codici dei modelli separati da virgola (es. '061-035, 091-051').
    - pascal_persi_impianto: estrai il numero associato alle parole 'Pascal', 'Pa' o 'perdita di carico' nel messaggio dell'utente (es. 250.0). Se non specificato, lascia 0.0."""
    print(f"\n[TOOL] Esecuzione VERIFICA_PREVALENZA -> Modelli: {codici_modelli} | Pascal: {pascal_persi_impianto}")

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
        if pascal_persi_impianto <= 0:
            risultati_finali.append(f"Modello {codice_pulito}: ha una prevalenza massima di {prev_float} Pa (richiesta non specificata).")
        else:
            if prev_float >= pascal_persi_impianto:
                risultati_finali.append(f"**Modello {codice_pulito}: COMPATIBILE.** Ha {prev_float} Pa, superiore ai {pascal_persi_impianto} Pa richiesti.")
            else:
                risultati_finali.append(f"**Modello {codice_pulito}: NON COMPATIBILE.** Ha solo {prev_float} Pa, insufficiente per i {pascal_persi_impianto} Pa richiesti.")

    # unisce i risultati
    testo_ritorno = ""
    for r in risultati_finali:
        testo_ritorno = testo_ritorno + r + "\n\n"

    # blocco di sicurezza per evitare loop
    testo_ritorno = testo_ritorno + "=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: Il calcolo della prevalenza è completato! ORA FERMATI. NON chiamare nessun altro tool (vietato usare il dizionario o altri calcoli). Scrivi immediatamente la risposta finale all'utente dicendo chiaramente se il modello è COMPATIBILE o NON COMPATIBILE."
    
    return testo_ritorno

@tool
def consulta_dizionario_catalogo(parola_chiave: str = "") -> str:
    """Usa ESCLUSIVAMENTE questo tool per rispondere a domande su COSA c'è nel catalogo, sul SIGNIFICATO delle caratteristiche, o su COME vengono misurati i parametri (es. rumorosità, decibel, tipo di gas, alimentazione, trifase).
    REGOLA: Se l'utente NON chiede un calcolo matematico, ma chiede spiegazioni discorsive, usa sempre questo tool.
    PARAMETRI:
    - parola_chiave: (Opzionale) La parola da cercare (es. 'rumorosità', 'gas'). Lascia vuoto per leggere tutto."""
    print(f"\n[TOOL] Esecuzione CONSULTA_DIZIONARIO -> Ricerca: '{parola_chiave}'")

    # Percorso relativo sicuro
    cartella_corrente = os.path.dirname(os.path.abspath(__file__))
    percorso_file = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

    try:
        with open(percorso_file, 'r', encoding='utf-8') as f:
            contenuto = f.read()
    except FileNotFoundError:
        return "Errore: Il file del dizionario non è stato trovato. Avvisa l'utente."

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
            
            return testo_ritorno + "=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: Hai trovato i dati! ORA FERMATI. NON chiamare più nessun tool. Scrivi immediatamente la risposta finale all'utente usando SOLO questi dati."
        else:
            return f"Nessuna voce trovata per '{parola_chiave}'. Ecco il dizionario:\n\n{contenuto}\n\n=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: ORA FERMATI. NON chiamare più nessun tool. Scrivi la risposta finale all'utente e concludi."

    return f"Ecco il dizionario completo:\n\n{contenuto}\n\n=== STOP TOOL ===\nORDINE TASSATIVO PER L'AI: ORA FERMATI. NON chiamare più nessun tool. Scrivi la risposta finale all'utente e concludi."


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
   - Usa ESCLUSIVAMENTE 'cerca_catalogo_specifico'.

6. IF l'utente fa domande su manutenzione, filtri, installazione o "vapori/grassi":
   - Usa ESCLUSIVAMENTE 'cerca_manuali' o 'cerca_sito_web'.
                                              
7. IF l'utente chiede spiegazioni tecniche, definizioni, o chiede se un parametro esiste nel catalogo (es. "rumorosità", "gas R32", "come viene misurato"):
   - Usa 'consulta_dizionario_catalogo'. NON USARE tool matematici.

REGOLE GLOBALI:
- Rispondi SOLO in Italiano.
- NON inventare parametri. Se non li sai, chiedili.
- DIVIETO DI CALCOLO A VUOTO: Se l'utente fa una domanda puramente discorsiva e NON fornisce numeri (kW, mq, persone, Pascal), ti è ASSOLUTAMENTE VIETATO usare i tool di calcolo (termico, aria, elettrico, prevalenza). Usa solo il dizionario o rispondi a parole.
- DIVIETO DI JSON: È severamente vietato rispondere mostrando codice JSON grezzo all'utente.
- DIVIETO CHIAMATE MULTIPLE: Ti è ASSOLUTAMENTE VIETATO chiamare due tool contemporaneamente. Scegli UN SOLO tool alla volta, attendi il risultato, e poi rispondi all'utente.
- REGOLA ANTI-LOOP: Dopo aver ricevuto i dati da QUALSIASI tool, ti è ASSOLUTAMENTE VIETATO richiamare lo stesso tool o chiamarne altri. Devi IMMEDIATAMENTE formulare la risposta discorsiva per l'utente e fermarti.""")
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

tools = [cerca_catalogo_specifico, 
         cerca_catalogo_generico, 
         cerca_sito_web, cerca_manuali, 
         calcola_fabbisogno_termico, 
         calcola_portata_aria, 
         calcola_consumo_elettrico, 
         verifica_prevalenza_canali,
         consulta_dizionario_catalogo]

# configurazione LangGraph e LLM
# parametri aggiunti per limitare i consumi della cpu e della ram
llm = ChatOllama(model="qwen2.5:3b-instruct-q8_0", temperature=0, num_thread=4, num_ctx=2048)
llm_con_tools = llm.bind_tools(tools)

tool_node = ToolNode(tools)

workflow = StateGraph(AgentState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)

workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
workflow.add_edge("tools", "agent")

app = workflow.compile()