import streamlit as st
import fitz  # PyMuPDF: Gestione PDF e coordinate
from PIL import Image, ImageDraw
import os

# --- CONFIGURAZIONE ---
PDF_FOLDER = "pipeline/data/0-ingestion"
DB_PATH = "pipeline/data/2-processing/chroma_db_knowledge_base_pdf"

# Configurazione della pagina Streamlit
st.set_page_config(layout="wide", page_title="PDF Knowledge Base Inspector")

def get_pdf_list():
    """Restituisce la lista dei file PDF presenti nella cartella configurata."""
    if not os.path.exists(PDF_FOLDER):
        os.makedirs(PDF_FOLDER)
        return []
    return [f for f in os.listdir(PDF_FOLDER) if f.endswith('.pdf')]

def render_page_with_boxes(pdf_path, page_num):
    """
    Funzione Core di Debug:
    Disegna i Bounding Box (rettangoli) su ogni blocco di testo rilevato.
    
    Questo permette di verificare visivamente se il sistema sta leggendo
    correttamente le tabelle e i paragrafi.
    """
    doc = fitz.open(pdf_path)
    page = doc[page_num]

    # 1. Estrazione del Testo 
    text_content = page.get_text("text")

    # 2. Rendering dell'immagine per la visualizzazione
    zoom = 2
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    draw = ImageDraw.Draw(img)

    # 3. Disegno dei Bounding Box di controllo
    # get_text("blocks") restituisce le coordinate di ogni blocco
    blocks = page.get_text("blocks")
    
    for block in blocks:
        x0, y0, x1, y1, text, block_no, block_type = block
        
        # Disegno solo se è testo (block_type 0)
        if block_type == 0:
            # Scalo le coordinate in base allo zoom dell'immagine
            rect = [x0 * zoom, y0 * zoom, x1 * zoom, y1 * zoom]
            
            # Disegno un rettangolo verde (simbolo di "Dato Rilevato")
            draw.rectangle(rect, outline="green", width=3)

    return text_content, img, len(doc)

# --- INTERFACCIA UTENTE (UI) ---

st.title("Strumento di Ispezione PDF")
st.markdown("""
Questo tool permette di visualizzare il processo di **Extraction & Chunking**.
A sinistra viene mostrato il testo grezzo estratto, a destra la verifica spaziale (Bounding Boxes).
""")

# Barra laterale per la selezione dei file
st.sidebar.header("Selezione Documento")
pdf_files = get_pdf_list()

if not pdf_files:
    st.error(f"Nessun file PDF trovato nella cartella '{PDF_FOLDER}'.")
else:
    # Selezione del File
    selected_pdf = st.sidebar.selectbox("Scegli un file PDF", pdf_files)
    
    if selected_pdf:
        pdf_path = os.path.join(PDF_FOLDER, selected_pdf)
        
        # Apro il documento per sapere quante pagine ha
        doc_preview = fitz.open(pdf_path)
        total_pages = len(doc_preview)
        doc_preview.close()
        
        # Slider per navigare le pagine
        page_selection = st.sidebar.slider("Pagina", 1, total_pages, 1)
        page_index = page_selection - 1 

        # --- ELABORAZIONE VISIVA ---
        try:
            extracted_text, visual_check_img, _ = render_page_with_boxes(pdf_path, page_index)
            
            # Layout a due colonne
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Output Testuale (Markdown)")
                st.caption("Questo è il dato puro che viene indicizzato nel Database.")
                st.text_area("Contenuto estratto", extracted_text, height=800)
                
            with col2:
                st.subheader("Verifica Visiva (Bounding Box)")
                st.caption("I box VERDI indicano le aree di testo rilevate dal motore OCR/Parsing.")
                st.image(visual_check_img, use_container_width=True)
                
        except Exception as e:
            st.error(f"Errore durante l'elaborazione della pagina: {e}")