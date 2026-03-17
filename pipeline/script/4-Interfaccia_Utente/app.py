import sys
import os
import streamlit as st

percorso_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(percorso_root)
from prototype.davide.prova import elabora_richiesta

st.set_page_config(page_title="Zoppellaro AI", layout="wide", initial_sidebar_state="expanded")

if "user_id" not in st.session_state:
    st.session_state.user_id = "utente_01"

if "memoria_utenti" not in st.session_state:
    st.session_state.memoria_utenti = {}

if st.session_state.user_id not in st.session_state.memoria_utenti:
    st.session_state.memoria_utenti[st.session_state.user_id] = {
        "tutte_le_chat": {"Chat 1": []},
        "chat_attiva": "Chat 1",
        "contatore_chat": 1
    }

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
        st.rerun()
        
    for nome_chat in list(dati_utente["tutte_le_chat"].keys()):
        if st.button(f"{nome_chat}", key=f"{st.session_state.user_id}_{nome_chat}", use_container_width=True):
            dati_utente["chat_attiva"] = nome_chat
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
    with st.chat_message("user"):
        st.write(user_query)

    with st.chat_message("assistant"):
        with st.spinner("Il motore AI sta elaborando la richiesta..."):
            try:
                response = elabora_richiesta(user_query)
                
                st.write(response["testo"])
                
                if response["azioni"]:
                    st.info(f"Il motore ha consultato i tool: {', '.join(response['azioni'])}")
                
                cronologia_corrente.append({
                    "role": "assistant", 
                    "content": response["testo"],
                    "azioni": response["azioni"]
                })
                
            except Exception as e:
                st.error(f"Si è verificato un errore nel motore: {e}")
