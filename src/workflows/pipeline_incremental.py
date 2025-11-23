# src/workflows/pipeline_incremental.py
# Pipeline avec sauvegarde page par page + gestion des crashs

import csv
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    StaleElementReferenceException, 
    ElementClickInterceptedException,
    InvalidSessionIdException,
    WebDriverException
)
from bs4 import BeautifulSoup
from src.config import TRUSTPILOT_URL, CSV_OUTPUT_RAW

def parse_page_reviews(html):
    """Parse une seule page HTML et retourne les avis"""
    soup = BeautifulSoup(html, "html.parser")
    reviews = soup.find_all("section", class_="styles_reviewContentwrapper__K2aRu")
    
    page_reviews = []
    for r in reviews:
        # Auteur
        author_tag = r.find("span", class_="styles_consumerName__dS7aM")
        author = author_tag.get_text(strip=True) if author_tag else "Anonyme"

        # Date
        date_tag = r.find("time")
        if date_tag and date_tag.get("datetime"):
            date = date_tag["datetime"]
        else:
            date_text_tag = r.find("span", class_="CDS_Badge_badgeText__2f6174")
            date = date_text_tag.get_text(strip=True) if date_text_tag else "Date inconnue"

        # Note
        rating_tag = r.find("div", {"data-service-review-rating": True})
        try:
            note = int(rating_tag["data-service-review-rating"]) if rating_tag else "N/A"
        except (ValueError, TypeError):
            note = "N/A"

        # Commentaire
        comment_tag = r.find("p", class_="CDS_Typography_body-l__dd9b51")
        if not comment_tag:
            comment_tag = r.find("p", {"data-service-review-text-typography": "true"})
        comment = comment_tag.get_text(strip=True) if comment_tag else "Pas de commentaire"

        page_reviews.append({
            "Auteur": author,
            "Date": date,
            "Note": note,
            "Commentaire": comment
        })
    
    return page_reviews

def clean_duplicates_in_file(csv_path):
    """Enlever les doublons du fichier CSV final"""
    if not os.path.exists(csv_path):
        return 0
    
    seen = set()
    unique_rows = []
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            key = (row["Auteur"], row["Date"], row["Commentaire"])
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
    
    duplicates = len(seen) - len(unique_rows)
    
    # Réécrire le fichier sans doublons
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(unique_rows)
    
    return duplicates

def run_pipeline_safe():
    """Pipeline sécurisé avec sauvegarde incrémentale"""
    print("=" * 70)
    print("PIPELINE TRUSTPILOT - MODE SÉCURISÉ (Sauvegarde incrémentale)")
    print("=" * 70)
    
    # Préparer le fichier CSV
    keys = ["Auteur", "Date", "Note", "Commentaire"]
    os.makedirs(os.path.dirname(CSV_OUTPUT_RAW), exist_ok=True)
    
    # Créer le header si le fichier n'existe pas
    file_exists = os.path.exists(CSV_OUTPUT_RAW)
    if not file_exists:
        with open(CSV_OUTPUT_RAW, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
        print(f"✓ Fichier CSV créé: {CSV_OUTPUT_RAW}\n")
    else:
        print(f"Le fichier existe déjà, ajout des nouvelles données...\n")
    
    # Configuration Chrome
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(TRUSTPILOT_URL)
        time.sleep(3)
        
        # Accepter les cookies
        try:
            consent_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            consent_button.click()
            time.sleep(1)
        except TimeoutException:
            pass
        
        page_count = 1
        total_reviews = 0
        max_retries = 3
        
        while True:
            try:
                print(f"📄 Page {page_count}...", end=" ", flush=True)
                
                # Récupérer le HTML
                html = driver.page_source
                
                # Parser les avis
                reviews = parse_page_reviews(html)
                print(f"{len(reviews)} avis", end=" ")
                
                # Sauvegarder immédiatement dans le CSV
                if reviews:
                    with open(CSV_OUTPUT_RAW, "a", newline="", encoding="utf-8-sig") as f:
                        writer = csv.DictWriter(f, fieldnames=keys)
                        writer.writerows(reviews)
                    total_reviews += len(reviews)
                    print(f"→ ✓ Sauvegardé (Total: {total_reviews})")
                else:
                    print("→ Aucun avis trouvé")
                
                # Chercher le bouton suivant
                clicked = False
                for attempt in range(max_retries):
                    try:
                        next_button = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[aria-label='Page suivante']"))
                        )
                        
                        # Vérifier si désactivé
                        if 'disabled' in (next_button.get_attribute('class') or '') or \
                           next_button.get_attribute('aria-disabled') == 'true':
                            print("\n🏁 Dernière page atteinte !")
                            driver.quit()
                            
                            # Nettoyage final
                            print("\n[Nettoyage] Suppression des doublons...")
                            duplicates = clean_duplicates_in_file(CSV_OUTPUT_RAW)
                            if duplicates > 0:
                                print(f"✓ {duplicates} doublons supprimés")
                            
                            print(f"\n SUCCÈS: {total_reviews} avis sauvegardés dans {CSV_OUTPUT_RAW}")
                            return
                        
                        # Scroll et clic
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", next_button)
                        
                        # Attendre le chargement
                        WebDriverWait(driver, 10).until(
                            lambda d: d.execute_script("return document.readyState") == "complete"
                        )
                        time.sleep(2)
                        
                        clicked = True
                        page_count += 1
                        break
                        
                    except (StaleElementReferenceException, ElementClickInterceptedException) as e:
                        if attempt < max_retries - 1:
                            time.sleep(2)
                        else:
                            print(f"\n  Échec après {max_retries} tentatives")
                            break
                    
                    except TimeoutException:
                        print("\n Plus de bouton 'Page suivante'")
                        break
                
                if not clicked:
                    break
                    
            except (InvalidSessionIdException, WebDriverException) as e:
                print(f"\n Erreur Selenium: {type(e).__name__}")
                print(f" {total_reviews} avis déjà sauvegardés dans {CSV_OUTPUT_RAW}")
                print("  Vous pouvez relancer le script, il continuera là où il s'est arrêté")
                break
    
    finally:
        try:
            driver.quit()
        except:
            pass
    
    # Nettoyage final
    print("\n[Nettoyage] Suppression des doublons...")
    duplicates = clean_duplicates_in_file(CSV_OUTPUT_RAW)
    if duplicates > 0:
        print(f" {duplicates} doublons supprimés")
    
    print(f"\n {total_reviews} avis sauvegardés dans {CSV_OUTPUT_RAW}")
    print("=" * 70)

if __name__ == "__main__":
    run_pipeline_safe()