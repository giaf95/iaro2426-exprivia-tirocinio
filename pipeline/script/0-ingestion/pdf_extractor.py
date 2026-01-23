import os
import tkinter as tk
from tkinter import filedialog
import pandas as pd
import pdfplumber

def start_dinamico():
    """
    Funzione principale per l'estrazione dati da PDF con auto-mappatura delle unità scoperta dinamicamente.
    Include selezione file GUI, processamento tabelle, pulizia dati e export Excel.
    """
    print("AVVIO ESTRAZIONE (Auto-Mappatura Unità attiva)...")
    
    # --- 1. CONFIGURAZIONE E INTERFACCIA UTENTE ---
    # Definiamo le unità che lo script deve "imparare" a riconoscere nel PDF
    UNITA_DA_CERCARE = ['m3/h', 'Pa', 'kW', '%']
    
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    # Selezione file PDF
    path_pdf = filedialog.askopenfilename(
        title="SELEZIONA PDF", 
        filetypes=[("PDF", "*.pdf")]
    )
    root.destroy() # Chiude l'istanza di tkinter
    
    if not path_pdf: 
        return

    # Setup percorsi output
    cartella = os.path.dirname(path_pdf)
    nome_output = os.path.join(cartella, "CATALOGO_FINALE_UNITA.xlsx")
    
    # Dizionario che verrà popolato automaticamente leggendo il PDF
    mappa_unita_automatica = {} 
    tutti_i_dati = []

    # --- 2. CARICAMENTO MAPPATURA (AUTOMATICO) ---
    # Rimossa la necessità di caricare il file ODS esterno

    # --- 3. PROCESSAMENTO PDF ---
    with pdfplumber.open(path_pdf) as pdf:
        for i, pagina in enumerate(pdf.pages):
            tabel_le = pagina.extract_tables()
            
            for tabella in tabel_le:
                if not tabella or len(tabella) < 2: 
                    continue
                
                # --- AUTO-SCOPERTA DELLE UNITA' ---
                # Scansioniamo la tabella per vedere se ci sono righe che dichiarano unità
                for riga_raw in tabella:
                    # Puliamo l'etichetta (prima colonna)
                    etichetta_clean = str(riga_raw[0]).replace('\n', ' ').strip().lower()
                    if etichetta_clean and etichetta_clean != "nan":
                        # Cerchiamo se in quella riga è presente un'unità nota
                        for cella in riga_raw:
                            val_unit = str(cella).strip()
                            if val_unit in UNITA_DA_CERCARE:
                                # Se la troviamo, mappiamo il nome della riga a quell'unità
                                mappa_unita_automatica[etichetta_clean] = val_unit

                # Creazione DataFrame temporaneo e pulizia base
                df_temp = pd.DataFrame(tabella).replace('\n', ' ', regex=True)
                df_temp = df_temp.replace(['', None], pd.NA)
                
                # Propagazione valori (Fill Forward)
                df_temp.iloc[:, 1:] = df_temp.iloc[:, 1:].ffill(axis=1)
                df_temp.iloc[:, 0] = df_temp.iloc[:, 0].ffill()

                # Gestione etichette vuote o duplicate nella prima colonna
                for idx in range(len(df_temp)):
                    val = str(df_temp.iloc[idx, 0]).strip()
                    df_temp.iloc[idx, 0] = val if val not in ["nan", ""] else f"Extra_{idx}"

                # Trasposizione tabella (Assunzione: Chiave -> Valore)
                df_temp = df_temp.drop_duplicates(subset=[0]).set_index(0).T
                df_temp["Pagina_PDF"] = i + 1
                
                # Conversione in dizionario per applicare la mappatura scoperta
                records = df_temp.to_dict(orient='records')

                for record in records:
                    nuove_unita = {}
                    for col_nome, valore in record.items():
                        col_clean = str(col_nome).strip().lower()
                        
                        # Controllo se la colonna esiste nella mappatura creata dinamicamente
                        if col_clean in mappa_unita_automatica:
                            nuove_unita[f"{col_nome} unit"] = mappa_unita_automatica[col_clean]
                    
                    if nuove_unita:
                        record.update(nuove_unita)
                
                tutti_i_dati.extend(records)

    # --- DEFINIZIONE FUNZIONI DI PULIZIA ---
    def mantieni_solo_numeri(valore):
        """Tenta di convertire il valore in float, altrimenti restituisce NA."""
        if pd.isna(valore): return pd.NA
        s_val = str(valore).strip()
        try:
            # Gestisce sia punto che virgola come separatori decimali
            float(s_val.replace(',', '.'))
            return s_val
        except ValueError:
            return pd.NA

    def riga_valida(valore):
        """
        True: Valore numerico o vuoto (mantiene la riga).
        False: Testo rilevato (es. 'KG'), elimina la riga.
        """
        if pd.isna(valore) or str(valore).strip() == "":
            return True 
        try:
            float(str(valore).replace(',', '.'))
            return True 
        except ValueError:
            return False 

    # --- 4. CREAZIONE EXCEL FINALE ---
    if tutti_i_dati:
        df_finale = pd.DataFrame(tutti_i_dati)
        # Rimuove colonne duplicate
        df_finale = df_finale.loc[:, ~df_finale.columns.duplicated()]

        # --- AUTOMATIZZAZIONE PULIZIA ---
        # Identifichiamo dinamicamente le colonne numeriche: 
        # escludiamo i metadati e le colonne delle unità appena create
        COLONNE_METADATI = ["Pagina_PDF", "Modello_Riferimento"]
        COLONNE_DA_PULIRE = [
            c for c in df_finale.columns 
            if c not in COLONNE_METADATI and not str(c).endswith(" unit")
        ]

        print("\n--- Inizio Pulizia Numeri (Automatica) ---")
        if COLONNE_DA_PULIRE:
            print("\n--- Analisi Righe Sporche ---")
            for col in COLONNE_DA_PULIRE:
                if col in df_finale.columns:
                    n_prima = len(df_finale)
                    
                    # Filtro: tieni solo righe valide
                    df_finale = df_finale[df_finale[col].apply(riga_valida)]
                    n_dopo = len(df_finale)
                    
                    if n_prima - n_dopo > 0:
                        print(f"Colonna '{col}': eliminate {n_prima - n_dopo} righe contenenti testo.")
                    
                    # Pulizia formale dei valori rimasti
                    df_finale[col] = df_finale[col].apply(mantieni_solo_numeri)
        
        print("--- Fine Analisi ---\n")

        # Pulizia righe completamente vuote (eccetto metadati)
        colonne_dati = [c for c in df_finale.columns if c != "Pagina_PDF"]
        df_finale = df_finale.dropna(subset=colonne_dati, how='all')

        # Ordinamento Colonne Intelligente
        colonne_ordinate = ["Pagina_PDF", "Modello_Riferimento"]
        altre = [c for c in df_finale.columns if c not in colonne_ordinate and " unit" not in c]
        
        percorso_finale = []
        # Aggiunge colonne prioritarie se esistono
        for c in colonne_ordinate:
            if c in df_finale.columns: percorso_finale.append(c)
            
        # Aggiunge le altre colonne seguite dalla loro unità
        for col in altre:
            percorso_finale.append(col)
            unit_col = f"{col} unit"
            if unit_col in df_finale.columns:
                percorso_finale.append(unit_col)
        
        # Filtro finale per garantire esistenza colonne
        percorso_finale = [c for c in percorso_finale if c in df_finale.columns]
        df_finale = df_finale[percorso_finale]

        # --- 5. SALVATAGGIO ---
        try:
            df_finale.to_excel(nome_output, index=False)
            print(f"FATTO! Trovati {len(df_finale)} prodotti.")
            print(f"File salvato: {nome_output}")
        except PermissionError:
            print("\n!!! ERRORE DI PERMESSO !!!")
            print(f"Sembra che il file '{nome_output}' sia aperto.")
            input("CHIUDI IL FILE EXCEL e premi INVIO qui per riprovare...")
            df_finale.to_excel(nome_output, index=False)
            print("Salvato con successo.")
            
    else:
        print("Nessun dato trovato.")

if __name__ == "__main__":
    start_dinamico()