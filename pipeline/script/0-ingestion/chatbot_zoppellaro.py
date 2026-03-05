import os
import textwrap
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

# --- CONFIGURAZIONE ---
PERSIST_DIR = "./chroma_db_zoppellaro"
SCORE_THRESHOLD = 1.2 
WEBSITE_URL = "https://www.zoppellaro.net" 

def load_system():
    print("--- CHATBOT ZOPPELLARO (VERSIONE ESAME) ---")
    
    if not os.path.exists(PERSIST_DIR):
        print(f"ERRORE: Non trovo il database in {PERSIST_DIR}")
        return None

    embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    
    # lettura dei dati
    vectorstore = Chroma(
        persist_directory=PERSIST_DIR,
        embedding_function=embeddings,
        collection_name="zoppellaro_kb"
    )
    
    print("SISTEMA PRONTO!")
    print("-" * 50)
    return vectorstore

def chat_loop(vectorstore):
    print("CHATBOT: Chiedi pure (es. 'Sale operatorie', 'Recuperatori').")
    print("(Scrivi 'esci' per chiudere)")
    print("-" * 50)
    
    while True:
        query = input("\nTU: ")
        if query.lower() in ["esci", "exit", "quit"]:
            print("Chatbot spento.")
            break
            
        print("   (Analisi semantica in corso...)")
        
        # Cerca 4 risultati
        results_with_score = vectorstore.similarity_search_with_score(query, k=4)
        
        # Filtra i risultati
        valid_results = []
        seen_texts = set()

        for doc, score in results_with_score:
            if score < SCORE_THRESHOLD:
                first_chars = doc.page_content[:50]
                if first_chars not in seen_texts:
                    valid_results.append(doc)
                    seen_texts.add(first_chars)
        
        # --- CASO: NESSUN RISULTATO ---
        if not valid_results:
            print(f"\nCHATBOT: Nessun risultato pertinente trovato.")
            print(f"Prova a cercare sul sito: {WEBSITE_URL}")
            continue

        # --- CASO: RISULTATI TROVATI ---
        print(f"\nCHATBOT: Ho trovato {len(valid_results)} documenti pertinenti:\n")
        
        for i, doc in enumerate(valid_results):
            fonte = doc.metadata.get("source_type", "CATALOGO").upper()
            prodotto = doc.metadata.get("title", "N/A")
            contenuto = doc.page_content
            
            print(f"--- RISULTATO {i+1} ---")
            print(f"Prodotto: {prodotto}")
            print(f"Fonte:    {fonte}")
            print("Dettagli:")
            print(textwrap.fill(contenuto, width=80))
            print("-" * 30)

if __name__ == "__main__":
    db = load_system()
    if db:
        chat_loop(db)