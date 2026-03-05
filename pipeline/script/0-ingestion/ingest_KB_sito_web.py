import json
import os
from langchain_core.documents import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma

# --- CONFIGURAZIONE ---
INPUT_KB_JSON = "knowledge_base_v1.json"
PERSIST_DIR = "./chroma_db_zoppellaro"

def load_knowledge_base(json_path):
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"File non trovato: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def create_langchain_documents(raw_data):
    documents = []
    print("--- Conversione dati in Documenti LangChain ---")
    
    for item in raw_data:
        # Pulizia metadati (rimuove valori nulli)
        clean_metadata = {k: v for k, v in item['metadata'].items() if v is not None}
        
        doc = Document(
            page_content=item['text_content'],
            metadata=clean_metadata
        )
        documents.append(doc)
    
    print(f"Documenti pronti: {len(documents)}")
    return documents

def build_vector_db(documents):
    print("\n--- Inizio Embedding (Creazione Vettori) ---")
    print("Caricamento modello 'all-MiniLM-L6-v2'...")
    
    embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    
    print(f"Scrittura database in: {PERSIST_DIR}...")
    vectorstore = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        persist_directory=PERSIST_DIR,
        collection_name="zoppellaro_kb"
    )
    print("Database salvato con successo!")
    return vectorstore

if __name__ == "__main__":
    try:
        raw_data = load_knowledge_base(INPUT_KB_JSON)
        docs = create_langchain_documents(raw_data)
        build_vector_db(docs)
        print("\nOra esegui: chatbot_zoppellaro.py")
    except Exception as e:
        print(f"ERRORE: {e}")