import pandas as pd
import json
import os
import hashlib
from datetime import datetime

# --- CONFIGURAZIONE ---
INPUT_FILE = "zoppellaro_estrazzione_autonoma.csv" 
OUTPUT_KB = "knowledge_base_v1.json"

def generate_id(text):
    """Genera un ID univoco per ogni blocco di testo."""
    return hashlib.md5(text.encode()).hexdigest()

def process_csv_source(filename):
    print(f"--- FASE 1: Lettura File {filename} ---")
    data = []
    
    if not os.path.exists(filename):
        print(f"ERRORE: Non trovo il file '{filename}'!")
        return []

    try:
        # Lettura del CSV con gestione errori e encoding corretto
        df = pd.read_csv(filename, sep=';', encoding='utf-8-sig', on_bad_lines='skip')
        print(f"File letto correttamente. Righe totali: {len(df)}")
    except Exception as e:
        print(f"Errore lettura: {e}")
        return []

    count = 0
    skipped = 0
    
    for index, row in df.iterrows():
        # Pulizia stringhe
        desc = str(row.get('Descrizione', '')).strip()
        prod = str(row.get('Prodotto', 'Sconosciuto')).strip()
        cat = str(row.get('Categoria', 'Generale')).strip()
        url = str(row.get('URL', '')).strip()

        # Filtri di Qualità (Rimuove righe troppo corte o inutili)
        if len(desc) < 20 or "Navigazione HOME" in desc: 
            skipped += 1
            continue

        full_text_with_link = f"URL PRODOTTO: {url}\nNOME PRODOTTO: {prod}\nCATEGORIA: {cat}\n\nDESCRIZIONE:\n{desc}"

        # Creazione JSON
        entry = {
            # Generiamo l'ID sul testo completo
            "id": generate_id(full_text_with_link),
            "text_content": full_text_with_link,
            
            "metadata": {
                "source_type": "csv_catalogo",
                "title": prod,
                "category": cat,
                "url": url,
                "ingested_at": datetime.now().isoformat()
            }
        }
        data.append(entry)
        count += 1
    
    print(f"-> Convertite {count} righe (scartate {skipped} righe di rumore).")
    return data

if __name__ == "__main__":
    print("--- GENERATORE KNOWLEDGE BASE ZOPPELLARO ---")
    web_data = process_csv_source(INPUT_FILE)
    
    if web_data:
        with open(OUTPUT_KB, 'w', encoding='utf-8') as f:
            json.dump(web_data, f, indent=4, ensure_ascii=False)
        print(f"\nFILE CREATO: {OUTPUT_KB}")
        print("Ora esegui: ingest.py")
    else:
        print("\nERRORE: Nessun dato processato.")