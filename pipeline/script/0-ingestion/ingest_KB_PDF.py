import fitz     
import chromadb
from sentence_transformers import SentenceTransformer
import os

# --- CONFIGURAZIONE ---
PDF_FOLDER = "pipeline/data/0-ingestion" 
DB_PATH = "pipeline/data/2-processing/chroma_db_knowledge_base_pdf" 
PDF_DA_LEGGERE = ["MANUALE_RT_REV_12_IT.pdf", "ROOFTOP_ZPSTDRT00_IT_rev02.pdf"]

def extract_text_with_coordinates(pdf_path):
    """
    Legge il PDF e restituisce una lista di blocchi.
    Ogni blocco contiene: testo, numero pagina, e coordinate (bbox).
    """
    doc = fitz.open(pdf_path)
    extracted_data = []
    
    print(f"Elaborazione: {pdf_path}...")

    for page_num, page in enumerate(doc):
        # get_text("blocks") restituisce blocchi di testo con coordinate
        # Formato blocco: (x0, y0, x1, y1, testo, block_no, block_type)
        blocks = page.get_text("blocks")
        
        for block in blocks:
            x0, y0, x1, y1, text, block_no, block_type = block
            
            # block_type 0 = testo, 1 = immagine
            # Filtriamo testi troppo brevi (meno di 10 caratteri) per evitare numeri di pagina o sporcizia
            if block_type == 0 and len(text.strip()) > 10:
                cleaned_text = text.strip()
                extracted_data.append({
                    "text": cleaned_text,
                    "metadata": {
                        "source": os.path.basename(pdf_path), # Nome del file
                        "page": page_num,
                        "x0": x0, "y0": y0, "x1": x1, "y1": y1 # Coordinate
                    }
                })
    return extracted_data

def create_database():
    # 1. Inizializza il modello di embedding (Piccolo, veloce, locale)
    print("Caricamento modello di embedding...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # 2. Inizializza il Database su disco
    client = chromadb.PersistentClient(path=DB_PATH)
    
    # Se la collezione esiste già, la cancelliamo per ripartire da zero (facoltativo)
    try:
        client.delete_collection("pdf_kb")
    except:
        pass
        
    collection = client.create_collection(name="pdf_kb")
    
    # 3. Trova tutti i PDF nella cartella
    pdf_files = [f for f in os.listdir(PDF_FOLDER) if f in PDF_DA_LEGGERE]
    
    all_documents = []
    all_metadatas = []
    all_ids = []
    id_counter = 0

    # 4. Elabora ogni PDF
    for pdf_file in pdf_files:
        chunks = extract_text_with_coordinates(os.path.join(PDF_FOLDER, pdf_file))
        
        for chunk in chunks:
            all_documents.append(chunk['text'])
            all_metadatas.append(chunk['metadata'])
            all_ids.append(str(id_counter))
            id_counter += 1

    # 5. Genera i vettori e salva nel DB
    if all_documents:
        print(f"Salvataggio di {len(all_documents)} blocchi nel database...")
        # Chromadb usa il modello di default se non specificato, ma qui passiamo gli embeddings calcolati
        embeddings = model.encode(all_documents).tolist()
        
        collection.add(
            documents=all_documents,
            embeddings=embeddings,
            metadatas=all_metadatas,
            ids=all_ids
        )
        print("Fatto! Database creato con successo.")
    else:
        print("Nessun testo valido trovato nei PDF.")

if __name__ == "__main__":
    create_database()