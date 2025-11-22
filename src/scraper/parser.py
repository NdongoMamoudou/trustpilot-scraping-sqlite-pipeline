


# src/scraper/parser.py
# parser.py – extraire Auteur, Date, Note, Commentaire




# ============================================================================
# FICHIER 3: src/scraper/parser.py
# ============================================================================
from bs4 import BeautifulSoup

def parse_comments(pages_html):
    all_reviews = []

    for page_num, html in enumerate(pages_html, 1):
        soup = BeautifulSoup(html, "html.parser")
        reviews = soup.find_all("section", class_="styles_reviewContentwrapper__K2aRu")
        
        print(f"  → Page {page_num}: {len(reviews)} avis trouvés")
        
        for r in reviews:
            # Auteur
            author_tag = r.find("span", class_="styles_consumerName__dS7aM")
            author = author_tag.get_text(strip=True) if author_tag else "Anonyme"

            # Date - CORRECTION: chercher dans l'attribut time
            date_tag = r.find("time")
            if date_tag and date_tag.get("datetime"):
                date = date_tag["datetime"]
            else:
                # Fallback sur le texte visible
                date_text_tag = r.find("span", class_="CDS_Badge_badgeText__2f6174")
                date = date_text_tag.get_text(strip=True) if date_text_tag else "Date inconnue"

            # Note
            rating_tag = r.find("div", {"data-service-review-rating": True})
            try:
                note = int(rating_tag["data-service-review-rating"]) if rating_tag else None
            except (ValueError, TypeError):
                note = None

            # Commentaire - vérifier plusieurs classes possibles
            comment_tag = r.find("p", class_="CDS_Typography_body-l__dd9b51")
            if not comment_tag:
                # Essayer d'autres sélecteurs possibles
                comment_tag = r.find("p", {"data-service-review-text-typography": "true"})
            comment = comment_tag.get_text(strip=True) if comment_tag else "Pas de commentaire"

            all_reviews.append({
                "Auteur": author,
                "Date": date,
                "Note": note,
                "Commentaire": comment
            })
    
    print(f"\n✓ Total: {len(all_reviews)} commentaires extraits")
    return all_reviews