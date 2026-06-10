import sys
import os
import json
import sqlite3
import pandas as pd
import fitz
from PIL import Image
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

# --- Importazione di brain ---
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
cartella_script = os.path.abspath(os.path.join(cartella_corrente, '..'))
cartella_processing = os.path.join(cartella_script, '2-processing')
cartella_pipeline = os.path.abspath(os.path.join(cartella_script, '..'))
cartella_root = os.path.abspath(os.path.join(cartella_pipeline, '..')) # La root del progetto
sys.path.append(cartella_processing)
sys.path.append(cartella_pipeline)
sys.path.append(cartella_root)
from brain import elabora_richiesta #type: ignore


cartella_pipeline = os.path.abspath(os.path.join(cartella_script, '..'))
DB_FILE = os.path.join(cartella_pipeline, 'data', '3-user_interface', 'database_chat.db')
EXCEL_PRESENTAZIONE = os.path.join(cartella_pipeline, 'data', '1-preprocessing', 'catalogo.xlsx')
cartella_pdf = os.path.join(cartella_pipeline, 'data','0-ingestion')
cartella_file = os.path.join(cartella_pipeline, 'data','1-preprocessing')
lista_nera = ["provaRT19_IT01.pdf", "RT19_IT01.pdf"]
file_disponibili = {}
ispezzione_file = [cartella_pdf, cartella_file]

for cartella in ispezzione_file:
    for file in os.listdir(cartella):
        if file.endswith('.pdf') or file.endswith('.xlsx') or file.endswith('.csv'):
            if file not in lista_nera:
                percorso_completo = os.path.join(cartella, file)
                file_disponibili[file] = percorso_completo

@st.dialog("Scegli un file")
def scelta(percorso_scelto):
    if percorso_scelto.endswith('.xlsx'):
        excel = pd.read_excel(percorso_scelto)
        st.dataframe(excel, width="stretch")
    elif percorso_scelto.endswith('.csv'):
        csv = pd.read_csv(percorso_scelto, sep=';')
        st.dataframe(csv, width="stretch")
    elif percorso_scelto.endswith('.pdf'):
        documento = fitz.open(percorso_scelto)
        for numero_pagina in range(len(documento)):
            pagina = documento.load_page(numero_pagina)
            pixel = pagina.get_pixmap(matrix = fitz.Matrix(2, 2))
            immagine = Image.frombytes("RGB", [pixel.width, pixel.height], pixel.samples)
            st.image(immagine, caption=f"Pagina {numero_pagina + 1}",width="stretch")

        
opzione = st.selectbox("Seleziona un file da vedere", list(file_disponibili.keys()), index=None, placeholder="Scegli un file...")
if opzione is not None:
    percorso_completo = file_disponibili[opzione]
    scelta(percorso_completo)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS storico_chat (
            user_id TEXT,
            chat_id TEXT,
            testo_conversazione TEXT,
            PRIMARY KEY (user_id, chat_id)
        )
    ''')
    conn.commit()
    conn.close()

def carica_memoria_utente(user_id):
    """Estrae i dati di un utente specifico ordinati cronologicamente."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id, testo_conversazione FROM storico_chat WHERE user_id = ? ORDER BY ROWID ASC", (user_id,))
    righe = cursor.fetchall()
    conn.close()
    
    if not righe:
        return None
        
    memoria = {"tutte_le_chat": {}, "contatore_chat": len(righe)}
    for c_id, msg_json in righe:
        memoria["tutte_le_chat"][c_id] = json.loads(msg_json)
        memoria["chat_attiva"] = c_id 
    return memoria

def salva_memoria_utente(user_id, chat_id, testo_conversazioni):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
           INSERT OR REPLACE INTO storico_chat (user_id, chat_id, testo_conversazione)
           VALUES (?, ?, ?)
    ''', (user_id, chat_id, json.dumps(testo_conversazioni, ensure_ascii=False)))
    conn.commit()
    conn.close()


def rename_chat(user_id, vecchio_nome, nuovo_nome):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE storico_chat
        SET chat_id = ?
        WHERE user_id = ? AND chat_id = ?
    ''', (nuovo_nome, user_id, vecchio_nome))
    conn.commit()
    conn.close()

#------------- Nuova funzione Elimina chat ------------------
def elimina_chat(user_id, chat_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM storico_chat
        WHERE user_id = ? AND chat_id = ?
    ''', (user_id, chat_id))
    conn.commit()
    conn.close()
#----------------------------------------------------------
#--------------- Nuova funzione Elimina Utente ------------------
def elimina_utente(user_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM storico_chat
        WHERE user_id = ?
    ''', (user_id,))
    conn.commit()
    conn.close()
#----------------------------------------------------------

init_db()

st.set_page_config(page_title="Zoppellaro AI", layout="wide", initial_sidebar_state="expanded")

if "user_id" not in st.session_state:
    st.session_state.user_id = "utente_01"

if "ultimi_dati_estratti" not in st.session_state:
    percorso_csv_avvio = os.path.join(cartella_pipeline, 'data', '3-user_interface', 'dataframe_grafico.csv')
    # Se il file esiste fisicamente, lo pre-carichiamo all'avvio!
    if os.path.exists(percorso_csv_avvio):
        try:
            if os.path.exists(percorso_csv_avvio):
                try:
                    st.session_state.ultimi_dati_estratti = pd.read_csv(percorso_csv_avvio, sep=',')
                except Exception:
                    st.session_state.ultimi_dati_estratti = None
        except Exception:
            st.session_state.ultimi_dati_estratti = None
    else:
        st.session_state.ultimi_dati_estratti = None

if "memoria_utenti" not in st.session_state:
    st.session_state.memoria_utenti = {}
if st.session_state.user_id not in st.session_state.memoria_utenti:
    dati_db = carica_memoria_utente(st.session_state.user_id)
    if dati_db:
        st.session_state.memoria_utenti[st.session_state.user_id] = dati_db
    else:
        st.session_state.memoria_utenti[st.session_state.user_id] = {
            "tutte_le_chat": {"Chat 1": []},
            "chat_attiva": "Chat 1",
            "contatore_chat": 1
        }
        salva_memoria_utente(st.session_state.user_id, "Chat 1", [])

dati_utente = st.session_state.memoria_utenti[st.session_state.user_id]


st.title("Zoppellaro AI - Assistente Tecnico")
st.caption(f"Utente attivo: **{st.session_state.user_id}** | Chat attiva: **{dati_utente['chat_attiva']}**")

with st.sidebar:
    st.title("Menu")

    st.markdown("### Profilo Utente")
    nuovo_user_id = st.text_input("Inserisci il tuo User-ID:", value=st.session_state.user_id)
    if nuovo_user_id != st.session_state.user_id and nuovo_user_id.strip() != "":
        st.session_state.user_id = nuovo_user_id.strip()
        st.success(f"Passato all'utente: {st.session_state.user_id}")
        st.rerun() 
    
    st.divider()
    st.subheader("Storico Chat")
    
    if st.button("Nuova Chat", width="stretch"):
        dati_utente["contatore_chat"] += 1
        nuovo_nome = f"Chat {dati_utente['contatore_chat']}"
        dati_utente["tutte_le_chat"][nuovo_nome] = []
        dati_utente["chat_attiva"] = nuovo_nome
        salva_memoria_utente(st.session_state.user_id, nuovo_nome, [])
        st.rerun()

# Storico Chat scrollabile
    with st.container(height=250):
        for nome_chat in list(dati_utente["tutte_le_chat"].keys()):
            if st.button(f"{nome_chat}", key=f"{st.session_state.user_id}_{nome_chat}", width="stretch"):
                dati_utente["chat_attiva"] = nome_chat
                st.rerun()

    # 2. DOWNLOAD DATI ESTRATTI
    st.divider()
    st.subheader("Report Dati")
    if st.session_state.ultimi_dati_estratti is not None:
        df_export = st.session_state.ultimi_dati_estratti
        
        with st.expander("Anteprima dati"):
            st.dataframe(df_export, hide_index=True)
            
        csv = df_export.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')
        st.download_button(
            label="Scarica CSV",
            data=csv,
            file_name="estrazione_tecnica.csv",
            mime="text/csv",
            width="stretch"
        )
    else:
        st.info("Nessun dato estratto nell'ultima richiesta.")

    st.markdown("Rinomina Chat")
    nuovo_nome_input = st.text_input("Nuovo nome per la chat", value=dati_utente["chat_attiva"])
    if st.button("Rinomina chat"):
        vecchio_nome = dati_utente["chat_attiva"]
        nuovo_nome = nuovo_nome_input.strip()
        if nuovo_nome == vecchio_nome or nuovo_nome == "":
            st.info("Inserisci un nome valido e diverso da quello attuale.")
        elif nuovo_nome in dati_utente["tutte_le_chat"]:
            st.error("Esiste già una chat con questo nome! Scegline un altro.")
        else:
            rename_chat(st.session_state.user_id, vecchio_nome, nuovo_nome)
            dati_utente["tutte_le_chat"][nuovo_nome] = dati_utente["tutte_le_chat"].pop(vecchio_nome)
            dati_utente["chat_attiva"] = nuovo_nome
            st.success(f"Chat rinominata in: {nuovo_nome}")
            st.rerun()

#----------------- Nuova funzionalità Elimina chat ------------------
    st.markdown("Elimina Chat")
    if len(dati_utente["tutte_le_chat"]) == 1:
        st.error("Non puoi eliminare l'unica chat esistente. Crea prima una nuova chat.")
    else:
        lista_chat = list(dati_utente["tutte_le_chat"].keys())
        indice_ultima_chat = len(lista_chat) - 1
        chat_da_eliminare = st.selectbox(
            "Seleziona la chat che vuoi eliminare", 
            lista_chat,
            index=indice_ultima_chat
        )
        if st.button("Elimina chat"):
            elimina_chat(st.session_state.user_id, chat_da_eliminare)
            dati_utente["tutte_le_chat"].pop(chat_da_eliminare)
        
            if dati_utente["chat_attiva"] == chat_da_eliminare: 
                dati_utente["chat_attiva"] = list(dati_utente["tutte_le_chat"].keys())[-1]
                
            st.success(f"Chat '{chat_da_eliminare}' eliminata.")
            st.rerun()
#----------------------------------------------------------

#----------------- Nuova funzionalità Elimina utente ------------------
    st.markdown("Elimina Utente")
    if st.button("Elimina utente"):
        elimina_utente(st.session_state.user_id)
        st.session_state.memoria_utenti.pop(st.session_state.user_id, None)
        st.success(f"Utente '{st.session_state.user_id}' eliminato.")
        st.session_state.user_id = "utente_01"
        st.rerun()
#----------------------------------------------------------

chat_attiva = dati_utente["chat_attiva"]
cronologia_corrente = dati_utente["tutte_le_chat"][chat_attiva]

for msg in cronologia_corrente:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        
        if "azioni" in msg and msg["azioni"]:
            st.caption(f" Azioni compiute: {', '.join(msg['azioni'])}")

        if msg.get("dati_visivi"):
            dati = msg["dati_visivi"]

            # 1. Nuovi grafici salvati su file HTML
            if dati.get("tipo") == "grafico_html_file":
                percorso_file = dati.get("path")
                if percorso_file and os.path.exists(percorso_file):
                    try:
                        import streamlit.components.v1 as components
                        with open(percorso_file, 'r', encoding='utf-8') as f:
                            html_data = f.read()
                        components.html(html_data, height=500, scrolling=True)
                    except Exception as e:
                        st.error(f"Errore nel caricamento del file grafico: {e}")
                else:
                    st.info("Nota: Il file di questo grafico non è più disponibile.")

            elif dati.get("tipo") == "tabella":
                try:
                    df_visivo = pd.DataFrame(dati["dati"])
                    st.dataframe(df_visivo, width="stretch", hide_index=True)
                    st.session_state.ultimi_dati_estratti = df_visivo
                except Exception as e:
                    st.error(f"Errore nel rendering della tabella: {e}")

            # 2. Salvagente per i primissimi test in RAM
            elif dati.get("tipo") == "html_in_memory":
                codice = dati.get("codice_html")
                if codice:
                    import streamlit.components.v1 as components
                    components.html(codice, height=500, scrolling=True)
                else:
                    st.info("Grafico precedente non disponibile (cronologia obsoleta).")
            
            # 3. Vecchia logica per prepara_dati_grafico (Commentata)
            # elif "dati" in dati:
            #     df_visivo = pd.DataFrame(dati["dati"])
            #     if dati["tipo"] == "grafico_barre":
            #         figura = px.bar(df_visivo, x="Modello", y="Valore", title=dati.get("titolo", ""))
            #         st.plotly_chart(figura, width="stretch")
            #     elif dati["tipo"] == "tabella":
            #         st.markdown(f"**Tabella: {dati.get('titolo', '')}**")
            #         st.dataframe(df_visivo, width="stretch", hide_index=True)

#------------------- TEST ----------------------------------------------
#storico_chat_finto = [
   # {
        #"ruolo": "user",
        #"tipo_messaggio": "testo",
        #"contenuto": "Mostrami i dati estratti dal catalogo."
    #},
    #{
       # "ruolo": "assistant",
        #"tipo_messaggio": "tabella_excel",
        #"contenuto":EXCEL_PRESENTAZIONE
   # },
    #{
        #"ruolo": "user",
        #"tipo_messaggio": "testo",
        #"contenuto": "Fantastico! E puoi farmi anche un grafico a barre di questi dati, magari mostrando la distribuzione dei prodotti per categoria?"
    #},
    #{
        #"ruolo": "assistant",
        #"tipo_messaggio": "grafico_excel",
        #"contenuto": EXCEL_PRESENTAZIONE
    #}
#]

#for messaggio in storico_chat_finto:
    #with st.chat_message(messaggio["ruolo"]): 
        #if messaggio["tipo_messaggio"] == "testo":
            #st.markdown(messaggio["contenuto"])  
        #elif messaggio["tipo_messaggio"] == "tabella_excel":
            #df = pd.read_excel(messaggio["contenuto"])
            #st.dataframe(df, width="stretch") 
        #elif messaggio["tipo_messaggio"] == "grafico_excel":
           # df = pd.read_excel(messaggio["contenuto"])
            #figura = px.bar(df, x="Modello PAL", y="Portata Massima Mandata Standard", title="Analisi Portata per Modello")
            #st.plotly_chart(figura, width="stretch")
        #else:
            #st.markdown("Tipo di messaggio non riconosciuto.")
#-----------------------------------------------------------------------

user_query = st.chat_input("Fai una domanda tecnica sui prodotti o servizi Zoppellaro...")

if user_query:
    cronologia_corrente.append({"role": "user", "content": user_query})
    salva_memoria_utente(st.session_state.user_id, chat_attiva, cronologia_corrente)
    
    with st.chat_message("user"):
        st.write(user_query)

    with st.chat_message("assistant"):
        with st.spinner("Il motore AI sta elaborando la richiesta..."):
            try:
                id_chat_corrente = f"{st.session_state.user_id}_{chat_attiva}"
                response = elabora_richiesta(user_query, chat_id=id_chat_corrente)
                
                st.write(response["testo"])
                
                # --- LOGICA PER LA CHAT E LA SIDEBAR ---
                dati_appena_estratti = False

                if "Dati estratti e salvati" in response["testo"]:
                    percorso_csv = os.path.join(cartella_pipeline, 'data', '3-user_interface', 'dataframe_grafico.csv')
                    if os.path.exists(percorso_csv):
                        try:
                            df_estratto = pd.read_csv(percorso_csv, sep=',')
                            st.session_state.ultimi_dati_estratti = df_estratto
                            dati_appena_estratti = True
                        except Exception:
                            pass
                
                # Salvataggio della cronologia della chat
                nuovo_messaggio_assistente = {
                    "role": "assistant",
                    "content": response["testo"],
                    "azioni": response["azioni"],
                    "dati_visivi": response.get("dati_visivi")
                }

                # Anti-duplicato: non salvare due volte la stessa risposta consecutiva
                if not cronologia_corrente or cronologia_corrente[-1].get("role") != "assistant" \
                or cronologia_corrente[-1].get("content") != response["testo"]:
                    cronologia_corrente.append(nuovo_messaggio_assistente)
                    salva_memoria_utente(st.session_state.user_id, chat_attiva, cronologia_corrente)
                else:
                    # Se è identica all'ultima, aggiorno solo eventuali dati_visivi mancanti
                    if not cronologia_corrente[-1].get("dati_visivi") and response.get("dati_visivi"):
                        cronologia_corrente[-1]["dati_visivi"] = response["dati_visivi"]
                        salva_memoria_utente(st.session_state.user_id, chat_attiva, cronologia_corrente)
                
                if dati_appena_estratti:
                    st.rerun()
                # ---------------------------------------------------
                if response.get("dati_visivi"):
                    dati = response["dati_visivi"]

                    # Lettura da file HTML
                    if dati.get("tipo") == "grafico_html_file":
                        percorso_file = dati.get("path")
                        if percorso_file and os.path.exists(percorso_file):
                            try:
                                import streamlit.components.v1 as components
                                with open(percorso_file, 'r', encoding='utf-8') as f:
                                    html_data = f.read()
                                components.html(html_data, height=500, scrolling=True)
                            except Exception as e:
                                st.error(f"Errore nel caricamento del file grafico: {e}")
                        else:
                            st.info("Nota: Il file di questo grafico non è più disponibile.")

                    elif dati.get("tipo") == "tabella":
                        try:
                            df_visivo = pd.DataFrame(dati["dati"])
                            st.dataframe(df_visivo, width="stretch", hide_index=True)
                        except Exception as e:
                            st.error(f"Errore nel rendering della tabella: {e}")

                    #Salvagente per i vecchi messaggi in cronologia
                    elif dati.get("tipo") == "html_in_memory":
                        codice = dati.get("codice_html")
                        if codice:
                            import streamlit.components.v1 as components
                            components.html(codice, height=500, scrolling=True)
                        else:
                            st.info("Grafico precedente non disponibile (cronologia obsoleta).")
                    
                    #Salvagente per i vecchi messaggi in cronologia
                    elif dati.get("tipo") == "html_in_memory":
                        codice = dati.get("codice_html")
                        if codice:
                            import streamlit.components.v1 as components
                            components.html(codice, height=500, scrolling=True)
                        else:
                            st.info("Grafico precedente non disponibile (cronologia obsoleta).")
                    
                    # Vecchia logica per prepara_dati_grafico
                    # else:
                    #     df_visivo = pd.DataFrame(dati["dati"])
                    #     if dati["tipo"] == "grafico_barre":
                    #         figura = px.bar(df_visivo, x="Modello", y="Valore", title=dati["titolo"])
                    #         st.plotly_chart(figura, width="stretch")
                    #     elif dati["tipo"] == "tabella":
                    #         st.markdown(f"**Tabella: {dati['titolo']}**")
                    #         st.dataframe(df_visivo, width="stretch", hide_index=True)
                
            except Exception as e:
                st.error(f"Si è verificato un errore nel motore: {e}")