import sys
import os
import json
import sqlite3
import streamlit as st

# --- Importazione di brain ---
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
cartella_script = os.path.abspath(os.path.join(cartella_corrente, '..'))
cartella_processing = os.path.join(cartella_script, '2-processing')
sys.path.append(cartella_processing)
from brain import elabora_richiesta #type: ignore

cartella_pipeline = os.path.abspath(os.path.join(cartella_script, '..'))
DB_FILE = os.path.join(cartella_pipeline, 'data', '3-user_interface', 'database_chat.db')

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
    
    if st.button("Nuova Chat", use_container_width=True):
        dati_utente["contatore_chat"] += 1
        nuovo_nome = f"Chat {dati_utente['contatore_chat']}"
        dati_utente["tutte_le_chat"][nuovo_nome] = []
        dati_utente["chat_attiva"] = nuovo_nome
        salva_memoria_utente(st.session_state.user_id, nuovo_nome, [])
        st.rerun()
        
    for nome_chat in list(dati_utente["tutte_le_chat"].keys()):
        if st.button(f"{nome_chat}", key=f"{st.session_state.user_id}_{nome_chat}", use_container_width=True):
            dati_utente["chat_attiva"] = nome_chat
            st.rerun()

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
        chat_da_eliminare = st.selectbox("Seleziona la chat che vuoi eliminare", list(dati_utente["tutte_le_chat"].keys()))
        if st.button("Elimina chat"):
            elimina_chat(st.session_state.user_id, chat_da_eliminare)
            dati_utente["tutte_le_chat"].pop(chat_da_eliminare)
            if dati_utente["chat_attiva"] == chat_da_eliminare: 
                dati_utente["chat_attiva"] = list(dati_utente["tutte_le_chat"].keys())[0]
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
                if response["azioni"]:
                    st.info(f"Il motore ha consultato i tool: {', '.join(response['azioni'])}")
                
                cronologia_corrente.append({
                    "role": "assistant", 
                    "content": response["testo"],
                    "azioni": response["azioni"]
                })
                salva_memoria_utente(st.session_state.user_id, chat_attiva, cronologia_corrente)
                
            except Exception as e:
                st.error(f"Si è verificato un errore nel motore: {e}")