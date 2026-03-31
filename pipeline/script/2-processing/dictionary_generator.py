import pandas as pd
import os

# Prende la cartella in cui si trova QUESTO script
cartella_corrente = os.path.dirname(os.path.abspath(__file__))

# Costruisce il percorso per l'Excel
percorso_excel = os.path.join(cartella_corrente, "..", "..", "data", "1-preprocessing", "catalogo.xlsx")

# Costruisce il percorso di salvataggio del file di testo
percorso_output = os.path.join(cartella_corrente, "..", "..", "data", "3-user_interface", "dizionario_catalogo.txt")

def crea_scheletro_dizionario():
    print("Lettura del catalogo Excel in corso...")
    
    try:
        # Legge solo l'intestazione per essere velocissimo
        df = pd.read_excel(percorso_excel, nrows=0)
    except Exception as e:
        print(f"Errore nella lettura dell'Excel: {e}")
        return

    # Prende tutte le colonne saltando le prime 3
    colonne_utili = df.columns[3:]

    # Crea il contenuto del file di testo
    testo_dizionario = "DIZIONARIO DEL CATALOGO HVAC\nDi seguito l'elenco dei parametri tecnici tracciati nel database e il loro significato:\n\n"
    
    for colonna in colonne_utili:
        # Pulisce eventuali spazi extra nel nome della colonna
        nome_pulito = str(colonna).strip()
        testo_dizionario += f"{nome_pulito}: [Il parametro indica...]\n\n"

    # Salva il file nella cartella di destinazione
    try:
        with open(percorso_output, 'w', encoding='utf-8') as f:
            f.write(testo_dizionario)
        print(f"Successo! File salvato in: {percorso_output}")
        print("Apri il file generato e sostituisci le parentesi quadre con una breve descrizione per l'AI.")
    except Exception as e:
        print(f"Errore nel salvataggio del file: {e}")

if __name__ == "__main__":
    crea_scheletro_dizionario()