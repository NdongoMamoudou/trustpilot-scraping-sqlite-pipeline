

# cleaner.py – nettoyage simple (doublons, encodage)


# ============================================================================
# FICHIER 4: src/scraper/cleaner.py
# ============================================================================
def clean_comments(comments):
    clean_data = []
    seen = set()
    
    for c in comments:
        # Enlever doublons (basé sur auteur+date+commentaire)
        key = (c["Auteur"], c["Date"], c["Commentaire"])
        if key not in seen:
            seen.add(key)
            # Vérifier que la note est valide
            if c["Note"] is None:
                c["Note"] = "N/A"
            clean_data.append(c)
    
    removed = len(comments) - len(clean_data)
    print(f"✓ {len(clean_data)} commentaires nettoyés ({removed} doublons supprimés)")
    return clean_data