import os
from typing import Annotated, List, TypedDict
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from langchain_ollama import OllamaLLM
from langgraph.graph import StateGraph, END
from ingestion import load_embeddings 

# Definizione della struttura dati dello stato
class AgentState(TypedDict):
    query: str
    context: List[str]
    answer: str
    target_kb: str

# Configurazione LLM e database vettoriale
embeddings = load_embeddings()
llm = OllamaLLM(model="llama3", temperature=0) # Temperature 0 aumenta la precisione tecnica

vector_tecnico = Chroma(
    persist_directory="./chroma_db", 
    embedding_function=embeddings, 
    collection_name="catalogo_hvac"
)

# Nodo per la classificazione dell'intento (routing)
def router_node(state: AgentState):
    query = state['query'].lower()
    
    if any(word in query for word in ["differenza", "confronta", "correlazione", "rispetto a"]):
        return {"target_kb": "confronto"}
    elif any(word in query for word in ["portata", "prevalenza", "potenza", "modello"]):
        return {"target_kb": "tecnico"}
    return {"target_kb": "manuale"}

# Nodo per il recupero dei documenti dal database
def retrieve_node(state: AgentState):
    target = state['target_kb']
    k_results = 5 if target == "confronto" else 3
    docs = vector_tecnico.similarity_search(state['query'], k=k_results)
    
    # Includiamo i metadati (ID modello e Pagina) nel contesto per l'LLM
    context_enhanced = []
    for d in docs:
        m_id = d.metadata.get('modello_id', 'N/D')
        pag = d.metadata.get('pagina', 'N/D')
        content = f"[MODELLO: {m_id} | PAGINA PDF: {pag}] Dati: {d.page_content}"
        context_enhanced.append(content)
    
    return {"context": context_enhanced}

# Nodo per la generazione della risposta finale
def generator_node(state: AgentState):
    target = state['target_kb']
    contesto_str = "\n---\n".join(state['context'])
    
    # Prompt più rigido per evitare confusione tra unità di misura
    prompt_base = f"""Sei un assistente tecnico HVAC. USA SOLO i dati forniti per rispondere.
REGOLE DI RISPOSTA:
1. Sii matematicamente preciso: se chiedono una portata > 40 m3/h, verifica solo quel valore.
2. NON confondere m3/h (Portata) con kW (Potenza) o A (Corrente).
3. Cita sempre il [MODELLO ID] e la [PAGINA PDF] per ogni informazione fornita.
4. Se i dati non permettono un confronto diretto, ammettilo chiaramente.

DATI TECNICI DISPONIBILI:
{contesto_str}

"""
    
    if target == "confronto":
        prompt = prompt_base + f"Analizza e confronta i modelli trovati per rispondere a: {state['query']}. Evidenzia le correlazioni nelle prestazioni."
    else:
        prompt = prompt_base + f"Rispondi in modo conciso alla domanda: {state['query']}"
    
    response = llm.invoke(prompt)
    return {"answer": response}

# Costruzione del grafo di esecuzione
workflow = StateGraph(AgentState)

workflow.add_node("router", router_node)
workflow.add_node("retrieve", retrieve_node)
workflow.add_node("generate", generator_node)

workflow.set_entry_point("router")
workflow.add_edge("router", "retrieve")
workflow.add_edge("retrieve", "generate")
workflow.add_edge("generate", END)

app = workflow.compile()

# Esecuzione dei test
if __name__ == "__main__":
    print("\n--- TEST AGENTE HVAC ---")
    
    # Test Ricerca
    res1 = app.invoke({"query": "Quali modelli hanno una portata superiore a 40 m3/h?"})
    print(f"\n[TASK: RICERCA]\nRisposta: {res1['answer']}")

    # Test Confronto
    res2 = app.invoke({"query": "Che differenza c'è tra i modelli con portata 34 e quelli con 44?"})
    print(f"\n[TASK: CORRELAZIONE]\nRisposta: {res2['answer']}")