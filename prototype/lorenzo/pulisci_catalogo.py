import pandas as pd
import os
import re

def pulisci_catalogo(input_path, output_path):
    print(f"Avvio pulizia del catalogo da: {input_path}")

    try:
        # 1. Caricamento robusto (tenta utf-8, se fallisce passa a latin-1)
        try:
            df = pd.read_csv(input_path, sep=None, engine="python", encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(input_path, sep=None, engine="python", encoding="latin-1")
            
        print(f"File caricato. Dimensioni iniziali: {df.shape[0]} righe, {df.shape[1]} colonne.")

        # 2. Pulizia Nomi Colonne (Rimuove spazi multipli e spazi finali/iniziali)
        df.columns = df.columns.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        
        # Rinominare colonne specifiche con errori noti o duplicati
        if "Potenza frigorifera totale macchina6" in df.columns:
            df.rename(columns={"Potenza frigorifera totale macchina6": "Potenza frigorifera totale macchina"}, inplace=True)
            
        # Gestione duplicati: se ci sono due "COP compressori", rinomina il secondo
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        df.columns = cols

        # 3. Rimozione colonne "Unità di misura" che creano confusione
        colonne_da_tenere = []
        for col in df.columns:
            # Elimina colonne che contengono SOLO un valore come 'm3/h', 'Pa', 'kW', '%', 'A'
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) == 1 and str(unique_vals[0]).strip() in ['m3/h', 'Pa', 'kW', '%', 'A']:
                pass # Ignora la colonna (la elimina)
            else:
                colonne_da_tenere.append(col)
                
        df = df[colonne_da_tenere]
        print(f"Rimosse colonne di sole unità di misura. Colonne rimanenti: {len(df.columns)}")

        # 4. Correzione formato "Date" di Excel nella colonna dei circuiti
        if 'N. compressori / N. circuiti' in df.columns:
            mappa_date = {
                '01-gen': '1-1',
                '02-gen': '2-1',
                '02-feb': '2-2',
                '04-feb': '4-2'
            }
            # Applica la sostituzione solo per le celle che matchano i valori sballati di Excel
            df['N. compressori / N. circuiti'] = df['N. compressori / N. circuiti'].replace(mappa_date)

        # 5. Pulizia del testo nelle celle (spazi extra)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()

        # 6. Sostituzione virgola con punto per i decimali (formato internazionale)
        for col in df.columns:
            if df[col].dtype == 'object':
                # Applica solo a colonne tecniche (Potenza, Portata, EER, ecc.) per non rovinare nomi modello
                if any(x in col for x in ["Potenza", "Portata", "EER", "COP", "Capacita", "Pressione", "Corrente", "Temperatura"]):
                     # Se ci sono punti usati per le migliaia li togliamo, poi convertiamo la virgola in punto
                     df[col] = df[col].str.replace(r'\.', '', regex=True).str.replace(',', '.', regex=False)

        # 7. Rimozione righe e colonne vuote assolute
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)

        # 8. Salvataggio
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        # Usiamo il separatore ';' per evitare conflitti con eventuali virgole residue nei testi
        df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
        
        print(f"Pulizia completata! Dimensioni finali: {df.shape[0]} righe, {df.shape[1]} colonne.")
        print(f"File pulito salvato in: {output_path}")

    except FileNotFoundError:
        print(f"Errore: Il file di input non esiste nel percorso: {input_path}")
    except Exception as e:
        print(f"Errore durante la pulizia: {e}")

if __name__ == "__main__":
    # Calcolo dinamico dei percorsi
    # Lo script si trova in: /cartella_principale/prototype/lorenzo/
    cartella_script = os.path.dirname(os.path.abspath(__file__))
    
    # Risaliamo di due cartelle per arrivare alla root del progetto
    cartella_base = os.path.abspath(os.path.join(cartella_script, "..", ".."))
    
    # Percorsi esatti verso la cartella pipeline
    file_input = os.path.join(cartella_base, "pipeline", "data", "1-preprocessing", "catalogo_sintetico_completo.csv")
    file_output = os.path.join(cartella_base, "pipeline", "data", "1-preprocessing", "catalogo_pulito.csv")
    
    pulisci_catalogo(file_input, file_output)