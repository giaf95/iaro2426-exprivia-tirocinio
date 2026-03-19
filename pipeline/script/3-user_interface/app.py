import sys
import os
import json
import sqlite3
import streamlit as st

#importazione di brain
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
cartella_script = os.path.abspath(os.path.join(cartella_corrente, '..'))
cartella_processing = os.path.join(cartella_script, '2-processing')
sys.path.append(cartella_processing)
from brain import elabora_richiesta #type: ignore

DB_FILE = "database_chat.db"

def init_db():
    """Crea la tabella nel database se non esiste già."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessioni_utenti (
            user_id TEXT PRIMARY KEY,
            dati TEXT
        )
    ''')
    conn.commit()
    conn.close()

def carica_memoria_utente(user_id):
    """Estrae i dati di un utente specifico dal database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT dati FROM sessioni_utenti WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return json.loads(row[0])
    return None

def salva_memoria_utente(user_id, dati):
    """Salva o aggiorna i dati dell'utente nel database in modo sicuro."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO sessioni_utenti (user_id, dati)
        VALUES (?, ?)
    ''', (user_id, json.dumps(dati, ensure_ascii=False)))
    conn.commit()
    conn.close()

init_db()

st.set_page_config(page_title="Zoppellaro AI", layout="wide", initial_sidebar_state="expanded")

if "user_id" not in st.session_state:
    st.session_state.user_id = "utente_01"

if "memoria_utenti" not in st.session_state:
    st.session_state.memoria_utenti = {}

dati_salvati = carica_memoria_utente(st.session_state.user_id)

if dati_salvati:
    st.session_state.memoria_utenti[st.session_state.user_id] = dati_salvati
elif st.session_state.user_id not in st.session_state.memoria_utenti:
    st.session_state.memoria_utenti[st.session_state.user_id] = {
        "tutte_le_chat": {"Chat 1": []},
        "chat_attiva": "Chat 1",
        "contatore_chat": 1
    }
    salva_memoria_utente(st.session_state.user_id, st.session_state.memoria_utenti[st.session_state.user_id])

dati_utente = st.session_state.memoria_utenti[st.session_state.user_id]

st.title("Zoppellaro AI - Assistente Tecnico")
st.caption(f"Utente attivo: **{st.session_state.user_id}** | Chat attiva: **{dati_utente['chat_attiva']}**")

with st.sidebar:
    st.title("Menu")

    st.markdown("### Profilo Utente")
    nuovo_user_id = st.text_input("Inserisci il tuo User-ID:", value=st.session_state.user_id)
    if nuovo_user_id != st.session_state.user_id:
        st.session_state.user_id = nuovo_user_id
        st.success(f"Passato all'utente: {nuovo_user_id}")
        st.rerun() 
    
    st.divider()
    
    st.subheader("Storico Chat")
    
    if st.button("Nuova Chat", use_container_width=True):
        dati_utente["contatore_chat"] += 1
        nuovo_nome = f"Chat {dati_utente['contatore_chat']}"
        dati_utente["tutte_le_chat"][nuovo_nome] = []
        dati_utente["chat_attiva"] = nuovo_nome
        
        salva_memoria_utente(st.session_state.user_id, dati_utente) 
        st.rerun()
        
    for nome_chat in list(dati_utente["tutte_le_chat"].keys()):
        if st.button(f"{nome_chat}", key=f"{st.session_state.user_id}_{nome_chat}", use_container_width=True):
            dati_utente["chat_attiva"] = nome_chat
            salva_memoria_utente(st.session_state.user_id, dati_utente)
            st.rerun()

cronologia_corrente = dati_utente["tutte_le_chat"][dati_utente["chat_attiva"]]

for msg in cronologia_corrente:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if "azioni" in msg and msg["azioni"]:
            st.caption(f" Azioni compiute: {', '.join(msg['azioni'])}")

user_query = st.chat_input("Fai una domanda tecnica sui prodotti o servizi Zoppellaro...")

if user_query:
    cronologia_corrente.append({"role": "user", "content": user_query})
    salva_memoria_utente(st.session_state.user_id, dati_utente)
    
    with st.chat_message("user"):
        st.write(user_query)

    with st.chat_message("assistant"):
        with st.spinner("Il motore AI sta elaborando la richiesta..."):
            try:
                #crea un ID univoco nome utente + nome chat
                id_chat_corrente = f"{st.session_state.user_id}_{dati_utente['chat_attiva']}"
                
                #passa l'ID a brain
                response = elabora_richiesta(user_query, chat_id=id_chat_corrente)
                
                st.write(response["testo"])
                
                if response["azioni"]:
                    st.info(f"Il motore ha consultato i tool: {', '.join(response['azioni'])}")
                
                cronologia_corrente.append({
                    "role": "assistant", 
                    "content": response["testo"],
                    "azioni": response["azioni"]
                })
                
                salva_memoria_utente(st.session_state.user_id, dati_utente)
                
            except Exception as e:
                st.error(f"Si è verificato un errore nel motore: {e}")
