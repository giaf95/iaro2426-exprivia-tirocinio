import sys

# Diciamo a test_tool dove si trova il file brain_dspy.py
percorso_cervello = r"C:\Users\PC_A87\Desktop\Carricamento Progetti GIT\pipeline\script\2-processing"
sys.path.insert(0, percorso_cervello)

from brain_dspy import elabora_richiesta # type: ignore

print("Avvio test...")
risposta = elabora_richiesta("crea un grafico a barre usando i dati appena estratti, con modello prodotto sull'asse X e Portata Massima sull'asse Y", chat_id="test_terminale")

print("\nRISPOSTA DEL BOT:")
print(risposta["testo"])

from brain_dspy import salva_prompt_dspy # type: ignore

print("Chiamata funzione salvataggio...")
salva_prompt_dspy()