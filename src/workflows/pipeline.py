


# # pipeline.py – orchestrer le tout et sauvegarder CSV


# ============================================================================
# FICHIER 5: src/workflows/pipeline.py
# ============================================================================
import csv
from src.scraper.fetcher import get_all_pages
from src.scraper.parser import parse_comments
from src.scraper.cleaner import clean_comments
from src.config import TRUSTPILOT_URL, CSV_OUTPUT_RAW

def run_pipeline():
    print("=" * 60)
    print("DÉBUT DU PIPELINE TRUSTPILOT")
    print("=" * 60)
    
    # Étape 1: Récupération des pages HTML
    print("\n[1/4] Récupération des pages HTML...")
    html_pages = get_all_pages(TRUSTPILOT_URL)
    
    if not html_pages:
        print(" Erreur: Aucune page récupérée")
        return
    
    # Étape 2: Parsing des commentaires
    print("\n[2/4] Extraction des commentaires...")
    comments = parse_comments(html_pages)
    
    if not comments:
        print(" Erreur: Aucun commentaire extrait")
        return
    
    # Étape 3: Nettoyage
    print("\n[3/4] Nettoyage des données...")
    clean_data = clean_comments(comments)
    
    # Étape 4: Sauvegarde CSV
    print("\n[4/4] Sauvegarde dans CSV...")
    keys = ["Auteur", "Date", "Note", "Commentaire"]
    try:
        with open(CSV_OUTPUT_RAW, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(clean_data)
        print(f"✓ {len(clean_data)} commentaires sauvegardés dans {CSV_OUTPUT_RAW}")
    except Exception as e:
        print(f" Erreur lors de la sauvegarde: {e}")
        return
    
    print("\n" + "=" * 60)
    print("PIPELINE TERMINÉ AVEC SUCCÈS")
    print("=" * 60)

if __name__ == "__main__":
    run_pipeline()