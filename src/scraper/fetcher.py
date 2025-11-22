# ============================================================================
# FICHIER 2: src/scraper/fetcher.py
# ============================================================================
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
import time

def get_all_pages(url, max_retries=3):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(3)
    
    # Fermer le bandeau cookies si présent
    try:
        consent_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        consent_button.click()
        time.sleep(1)
    except TimeoutException:
        pass
    
    all_pages = []
    page_count = 1
    
    while True:
        print(f"Récupération de la page {page_count}...")
        html = driver.page_source
        all_pages.append(html)
        
        # Vérifier s'il y a une page suivante
        clicked = False
        for attempt in range(max_retries):
            try:
                # Re-chercher l'élément à chaque tentative
                next_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[aria-label='Page suivante']"))
                )
                
                # Vérifier si le bouton est désactivé
                if 'disabled' in next_button.get_attribute('class') or \
                   next_button.get_attribute('aria-disabled') == 'true':
                    print("Dernière page atteinte (bouton désactivé)")
                    driver.quit()
                    return all_pages
                
                # Scroll vers le bouton
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                time.sleep(1)
                
                # Cliquer avec JavaScript
                driver.execute_script("arguments[0].click();", next_button)
                
                # Attendre le chargement de la nouvelle page
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(2)
                
                clicked = True
                page_count += 1
                break
                
            except (StaleElementReferenceException, ElementClickInterceptedException) as e:
                if attempt < max_retries - 1:
                    print(f"Tentative {attempt + 1}/{max_retries} échouée: {type(e).__name__} - Nouvelle tentative...")
                    time.sleep(2)
                else:
                    print(f"Échec après {max_retries} tentatives")
                    break
                
            except TimeoutException:
                print("Bouton 'Page suivante' non trouvé - fin de pagination")
                break
        
        if not clicked:
            break
    
    driver.quit()
    print(f"{len(all_pages)} pages récupérées au total")
    return all_pages
