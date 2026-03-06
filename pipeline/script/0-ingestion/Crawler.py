import time
import csv
import os
import re
import requests
import xml.etree.ElementTree as ET
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By

# ==============================================================================
# CONFIGURAZIONE (Minimalista & Future-Proof)
# ==============================================================================
CONFIG = {
    "DOMAIN": "zoppellaro.net",
    "SITEMAP_URL": "https://www.zoppellaro.net/sitemap.xml",
    "OUTPUT_FILE": "pipeline/data/1-preprocessing/zoppellaro_estrazzione_autonoma_.csv",
}

# ==============================================================================
# 1. CRAWLER "AGNOSTICO"
# ==============================================================================
class Crawler:
    def get_urls(self):
        print(f"--- [CRAWLER] Lettura Sitemap: {CONFIG['SITEMAP_URL']} ---")
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; IntelligentBot/3.0)'}
            response = requests.get(CONFIG['SITEMAP_URL'], headers=headers)
            if response.status_code == 200:
                urls = []
                root = ET.fromstring(response.content)
                ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                for loc in root.findall('.//ns:loc', ns):
                    url = loc.text.strip()
                    # Filtro tecnico di base (solo pagine web, niente PDF/Immagini)
                    if CONFIG['DOMAIN'] in url and not url.endswith((".pdf", ".jpg", ".png", ".css", ".xml")):
                        urls.append(url)
                unique = list(set(urls))
                print(f"--- [CRAWLER] Trovati {len(unique)} URL totali (Lingua ancora ignota) ---")
                return unique
            return []
        except Exception as e:
            print(f"Errore Sitemap: {e}")
            return []

# ==============================================================================
# 2. ESTRATTORE
# ==============================================================================
class IntelligentExtractor:
    def __init__(self):
        opts = Options()
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_argument("--disable-blink-features=AutomationControlled")
        self.driver = webdriver.Edge(options=opts)

    def close(self):
        self.driver.quit()

    def is_italian_page(self):
        """
        Determina se la pagina è italiana controllando SOLO il tag HTML standard.
        Principio KISS (Keep It Simple, Stupid).
        """
        try:
            html_elem = self.driver.find_element(By.TAG_NAME, "html")
            lang_attr = html_elem.get_attribute("lang")
            
            # Se il tag esiste e contiene 'it' (es. "it", "it-IT"), procediamo.
            if lang_attr and 'it' in lang_attr.lower():
                return True
            return False
        except:
            # In caso di errore di lettura DOM, scartiamo per sicurezza.
            return False

    def analyze(self, url):
        data = []
        try:
            self.driver.get(url)
            if not self.is_italian_page():
                return []
            
            # --- ESTRAZIONE DATI ---
            try: cat = self.driver.find_element(By.TAG_NAME, "h1").text.strip()
            except: cat = "Generale"

            # 1. RECUPERATORI "DNA" (Nascosti nelle Gallerie)
            galleries = self.driver.find_elements(By.CSS_SELECTOR, "div.caption-container")
            for gal in galleries:
                try:
                    t = self.driver.execute_script("return arguments[0].textContent;", gal.find_element(By.CSS_SELECTOR, ".caption-title")).strip()
                    d = self.driver.execute_script("return arguments[0].textContent;", gal.find_element(By.CSS_SELECTOR, ".caption-text")).strip()
                    # Pulizia stringhe
                    t = " ".join(t.split())
                    d = " ".join(d.split()).replace("Button", "")
                    
                    if self._is_prod(t, cat) and self._is_valid_desc(d):
                        data.append([cat, t, url, d])
                except: continue

            # 2. PRODOTTI "PCA" (Liste puntate nel testo)
            try: body = self.driver.find_element(By.TAG_NAME, "body").text 
            except: body = ""
            
            if "•" in body:
                bullets = self.driver.find_elements(By.XPATH, "//*[contains(text(), '•')]")
                for b in bullets:
                    try:
                        raw = b.text.strip()
                        lines = raw.splitlines() if "\n" in raw else [raw]
                        for line in lines:
                            if line.count('•') == 1:
                                parts = line.split('•', 1)
                                t = parts[0].strip().replace("PCA", "").strip()
                                d = parts[1].strip()
                                if self._is_prod(t, cat):
                                    data.append([cat, t, url, d])
                    except: continue

            # 3. PRODOTTI STANDARD (H2, H3, Grassetto)
            elems = self.driver.find_elements(By.CSS_SELECTOR, "h2, h3, h4, strong, b, p, li")
            curr = None
            buff = []
            
            for el in elems:
                if not el.is_displayed(): continue
                txt = " ".join(el.text.split())
                if not txt: continue
                
                # Stop al footer
                if "p. iva" in txt.lower() or "designed by" in txt.lower(): break
                
                tag = el.tag_name.lower()
                is_title = (tag in ['h2','h3','h4']) or (tag in ['strong','b'] and len(txt)<100)
                
                # Regole per salvare i casi limite
                if txt == "PCA" or "serie rpz" in txt.lower(): is_title = True
                if is_title and not self._is_prod(txt, cat): is_title = False
                
                if is_title:
                    if curr and buff:
                        full = " ".join(buff)
                        if self._is_valid_desc(full) and not any(x[1]==curr for x in data):
                            data.append([cat, curr, url, full])
                    curr = txt
                    buff = []
                else:
                    if curr and txt != curr: buff.append(txt)
            
            # Salvataggio ultimo buffer
            if curr and buff:
                full = " ".join(buff)
                if self._is_valid_desc(full) and not any(x[1]==curr for x in data):
                    data.append([cat, curr, url, full])

        except Exception as e: 
            print(f"Errore su {url}: {e}")
            
        return data

    def _is_prod(self, t, cat):
        # Filtri base per escludere il menu (sempre presenti)
        t = t.strip()
        bad_words = ["menu", "home", "chi siamo", "contatti", "privacy", "cookie", "login"]
        if len(t) < 3 or len(t) > 100: return False
        if t.lower() == cat.lower(): return False
        if any(x in t.lower() for x in bad_words): return False
        if t == "PCA": return True
        if t[0] in ['-', '.', '•']: return False
        return True

    def _is_valid_desc(self, t):
        if not t or len(t) < 15: return False
        if "@" in t: return False
        return True

# ==============================================================================
# 3. ESECUZIONE
# ==============================================================================
if __name__ == "__main__":
    print("--- AVVIO DEL CRAWLER---")
    
    # 1. Crawler
    crawler = Crawler()
    urls = crawler.get_urls()
    
    if urls:
        # 2. Extractor
        extractor = IntelligentExtractor()
        try:
            with open(CONFIG['OUTPUT_FILE'], 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(['Categoria', 'Prodotto', 'URL', 'Descrizione'])
                
                total = len(urls)
                for i, url in enumerate(urls):
                    print(f"({i+1}/{total}) Analisi: {url}")
                    
                    # L'Extractor decide internamente se la pagina è utile (IT) o no
                    prods = extractor.analyze(url)
                    
                    if prods:
                        # Deduplica locale
                        unique = []
                        seen = set()
                        for p in prods:
                            k = (p[1], p[3][:20])
                            if k not in seen:
                                seen.add(k)
                                unique.append(p)
                        
                        print(f"   -> TROVATI {len(unique)} prodotti.")
                        writer.writerows(unique)
                    else:
                        # Se vuoto, significa che era senza prodotti
                        pass
        finally:
            extractor.close()
            print(f"--- FINE. File salvato: {CONFIG['OUTPUT_FILE']} ---")