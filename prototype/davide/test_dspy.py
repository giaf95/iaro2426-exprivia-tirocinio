import pandas as pd
import dspy
from dspy.teleprompt import BootstrapFewShot

lm_locale = dspy.LM(
    'ollama/qwen2.5:7b-instruct-q4_K_M', 
    api_base='http://localhost:11434', 
    temperature=0.0 
)
dspy.configure(lm=lm_locale)

file_path = r"C:\Users\PC_A87\Desktop\Carricamento Progetti GIT\pipeline\data\1-preprocessing\catalogo_sintetico_completo.csv"
print("1. Caricamento del database...")

df_catalogo = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip', sep=';')
df_catalogo.columns = df_catalogo.columns.str.strip()

df_catalogo['Portata Massima'] = (
    df_catalogo['Portata Massima']
    .astype(str)
    .str.replace('.', '', regex=False)
    .str.replace(',', '.', regex=False)
)
df_catalogo['Portata Massima'] = pd.to_numeric(df_catalogo['Portata Massima'], errors='coerce')
colonne_per_ia = ['Modello Prodotto', 'Grandezza Telaio', 'Portata Massima', 'Pressione Operativa', 'Efficenza(%)']
esempio_dati = df_catalogo[colonne_per_ia].head(2).to_string(index=False)

print(f"Database pronto: {len(df_catalogo)} righe caricate.\n")

class GeneraFiltroReale(dspy.Signature):
    """Mappa la richiesta dell'utente con la colonna corretta del database fornito e scrivi ESATTAMENTE E SOLO una riga di codice Pandas."""
    domanda = dspy.InputField(desc="Richiesta dell'utente in italiano")
    nomi_colonne = dspy.InputField(desc="Schema reale del CSV")
    campione_dati = dspy.InputField(desc="Esempio dei dati")
    codice_pandas = dspy.OutputField(desc="Una singola riga di codice che DEVE iniziare con 'df_filtrato = df_catalogo['")
    colonne_da_stampare = dspy.OutputField(desc="Lista in Python delle 3 o 4 colonne più rilevanti da far vedere all'utente in base alla sua domanda. Esempio: ['Modello Prodotto', 'Portata Massima']")


def metrica_valutazione(example, pred, trace=None):
    codice = pred.codice_pandas.replace('```python', '').replace('```', '').strip()
    voto = True
    motivo_bocciatura = ""
    if "pd.DataFrame" in codice or "import pandas" in codice:
        voto = False
        motivo_bocciatura = "Ha provato a inventare il database o importare pandas"
    elif "df_filtrato =" not in codice:
        voto = False
        motivo_bocciatura = "Non ha salvato nella variabile df_filtrato"
    elif len(codice.split('\n')) > 2:
        voto = False
        motivo_bocciatura = "Ha scritto troppe righe invece di una sola"
    print("\n" + "-"*40)
    print("VALUTAZIONE IN CORSO:")
    
    if voto:
        print("Voto: PROMOSSO (1.0)")
    else:
        print(f"Voto:  BOCCIATO (0.0) -> Motivo: {motivo_bocciatura}")
    print("-"*40 + "\n")
    return voto

trainset = [
    dspy.Example(
        domanda="Trova le macchine con portata oltre 5000",
        nomi_colonne=str(colonne_per_ia),
        campione_dati=esempio_dati,
        codice_pandas="df_filtrato = df_catalogo[df_catalogo['Portata Massima'] > 5000]"
    ).with_inputs('domanda', 'nomi_colonne', 'campione_dati'),
    
    dspy.Example(
        domanda="Filtra modelli con portata inferiore a 1000",
        nomi_colonne=str(colonne_per_ia),
        campione_dati=esempio_dati,
        codice_pandas="df_filtrato = df_catalogo[df_catalogo['Portata Massima'] < 1000]"
    ).with_inputs('domanda', 'nomi_colonne', 'campione_dati')
]

print("2. Avvio Teleprompter: DSPy sta addestrando il modello...")
ottimizzatore = BootstrapFewShot(metric=metrica_valutazione, max_labeled_demos=2)
assistente_compilato = ottimizzatore.compile(dspy.Predict(GeneraFiltroReale), trainset=trainset)
print("COMPILAZIONE COMPLETATA!\n")


domanda_test = "Cerco una via di mezzo: trovami macchine con una portata compresa tra 4000 e 8000, ma escludi categoricamente i telai di tipo B e C. E già che ci sei, mostrami solo Modello, Telaio e Pressione"

print(f"3. Esecuzione: '{domanda_test}'")
risposta = assistente_compilato(
    domanda=domanda_test, 
    nomi_colonne=str(colonne_per_ia),
    campione_dati=esempio_dati
)

codice_finale = risposta.codice_pandas.replace('```python', '').replace('```', '').strip()
codice_eseguibile = [riga for riga in codice_finale.split('\n') if "df_filtrato" in riga]
if codice_eseguibile:
    codice_eseguibile = codice_eseguibile[0]
else:
    codice_eseguibile = codice_finale

print(f"Codice Generato:\n{codice_eseguibile}\n")

try:
    local_env = {'df_catalogo': df_catalogo, 'pd': pd}
    exec(codice_eseguibile, globals(), local_env)
    if 'df_filtrato' in local_env:
        df_risultato = local_env['df_filtrato']
        if isinstance(df_risultato, pd.Series):
            print(f"Valore esatto trovato:\n{df_risultato.to_string()}")  
        elif len(df_risultato) == 1:
            print("Trovato un modello specifico:")
            colonne_tabella = eval(risposta.colonne_da_stampare)
            print(df_risultato[colonne_tabella].iloc[0].to_string()) 
        elif not df_risultato.empty:
            print(f"Trovati {len(df_risultato)} modelli. Ecco la tabella:")
            colonne_tabella = eval(risposta.colonne_da_stampare)
            print(df_risultato[colonne_tabella].head(10))
        else:
            print("Nessun modello corrisponde a questo filtro.")     
except Exception as e:
    print(f"Errore Pandas: {e}")

print("\n" + "="*70)
print("IL PROMPT:")
print("="*70)
lm_locale.inspect_history(n=1)