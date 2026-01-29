import os
import json
import pandas as pd
from pathlib import Path
from langchain_core.documents import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma

class CatalogoToDocuments:
    def __init__(self, df):
        self.df = df
        self.documents = []
    
    def _extract_parametri(self, row):
        parametri = {}
        for col in self.df.columns:
            if col.endswith(" unit") or col in ["Pagina_PDF", "Modello_Riferimento"]:
                continue
            unit_col = f"{col} unit"
            if unit_col in self.df.columns:
                valore = row[col]
                unita = row[unit_col]
                if pd.notna(valore) and pd.notna(unita):
                    parametri[col] = {"valore": valore, "unita": str(unita).strip()}
        return parametri
    
    def _create_description(self, parametri):
        parts = []
        for nome, data in parametri.items():
            nome_clean = nome.replace("_", " ").title()
            parts.append(f"{nome_clean}: {data['valore']} {data['unita']}")
        return " | ".join(parts)
    
    def transform(self):
        for idx, row in self.df.iterrows():
            modello_id = row.get("Modello_Riferimento", f"MOD_{idx}")
            pagina = row.get("Pagina_PDF", 1)
            parametri = self._extract_parametri(row)
            descrizione = self._create_description(parametri)
            
            if not descrizione.strip():
                continue
            
            doc = Document(
                page_content=descrizione,
                metadata={
                    "modello_id": str(modello_id),
                    "pagina": int(pagina),
                    "indice_riga": idx,
                    "parametri_json": json.dumps(parametri, default=str),
                    "num_parametri": len(parametri)
                }
            )
            self.documents.append(doc)
        return self.documents

def load_embeddings():
    models = ["all-MiniLM-L6-v2", "all-mpnet-base-v2"]
    for model in models:
        try:
            print(f"caricamento {model}...")
            embeddings = HuggingFaceEmbeddings(
                model_name=model,
                show_progress=False,
                model_kwargs={"trust_remote_code": True}
            )
            print(f"ok: {model}")
            return embeddings
        except:
            continue
    
    embeddings = HuggingFaceEmbeddings(
        show_progress=False,
        model_kwargs={"trust_remote_code": True}
    )
    return embeddings

def create_vectorstore(documents, embeddings, persist_dir="./chroma_db"):
    print(f"embedding {len(documents)} documenti...")
    Path(persist_dir).mkdir(exist_ok=True)
    
    vectorstore = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        persist_directory=persist_dir,
        collection_name="catalogo_hvac"
    )
    vectorstore.persist()
    print(f"salvato in {persist_dir}")
    return vectorstore

def test_search(vectorstore, query, k=2):
    print(f"\nquery: {query}")
    results = vectorstore.similarity_search(query, k=k)
    
    if not results:
        print("nessun risultato")
        return
    
    for i, doc in enumerate(results, 1):
        print(f"[{i}] {doc.metadata['modello_id']} (pagina {doc.metadata['pagina']})")
        print(f"    {doc.page_content[:100]}...")

if __name__ == "__main__":
    print("ingestion pipeline hvac\n")
    
    try:
        print("caricamento excel...")
        excel_path = "CATALOGO_FINALE_UNITA.xlsx"
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"file non trovato: {excel_path}")
        
        df = pd.read_excel(excel_path)
        print(f"ok: {len(df)} modelli, {len(df.columns)} colonne\n")
        
        print("trasformazione documenti...")
        transformer = CatalogoToDocuments(df)
        documents = transformer.transform()
        print(f"ok: {len(documents)} documenti\n")
        
        print("setup embedding...")
        embeddings = load_embeddings()
        
        print("\ncreazione vector store...")
        vectorstore = create_vectorstore(documents, embeddings)
        
        print("\ningestion completata\n")
        print("test retrieval:")
        test_search(vectorstore, "alta portata", k=2)
        test_search(vectorstore, "bassa potenza", k=2)
        test_search(vectorstore, "freecooling", k=2)
        
        metadata = {
            "num_documents": len(documents),
            "num_columns": len(df.columns),
            "embedding_model": "all-MiniLM-L6-v2",
            "vector_store": "Chroma"
        }
        with open("ingestion_metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)
        print("\nok: metadata salvato")
        
    except Exception as e:
        print(f"errore: {e}")
        import traceback
        traceback.print_exc()
