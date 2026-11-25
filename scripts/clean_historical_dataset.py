
from pathlib import Path

import pandas as pd

INPUT_FILE = Path("data/historical/wikipedia_pageviews.parquet")
OUTPUT_FILE = Path("data/historical/wikipedia_pageviews_clean.parquet")


def main():
    df = pd.read_parquet(INPUT_FILE)

    initial_count = len(df)

    # Nettoyer les titres d'articles manquants ou vides
    df["article"] = df["article"].astype("string").str.strip()
    df = df.dropna(subset=["article"])
    df = df[df["article"] != ""]

    # Convertir et vérifier les dates
    df["date"] = pd.to_datetime(
        df["date"], format="%Y/%m/%d", errors="coerce"
    )
    df = df.dropna(subset=["date"])

    # Vérifier les valeurs numériques
    df["views"] = pd.to_numeric(df["views"], errors="coerce")
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")

    df = df.dropna(subset=["views", "rank"])
    df = df[(df["views"] >= 0) & (df["rank"] > 0)]

    # Déduplication sur la clé métier
    df = df.drop_duplicates(
        subset=["article", "date"], keep="first"
    )

    df["views"] = df["views"].astype("int64")
    df["rank"] = df["rank"].astype("int64")

    df = df.sort_values(["date", "article"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)

    print("Nettoyage terminé")
    print(f"Lignes initiales : {initial_count}")
    print(f"Lignes conservées : {len(df)}")
    print(f"Lignes supprimées : {initial_count - len(df)}")
    print(f"Articles distincts : {df['article'].nunique()}")
    print(f"Dates distinctes : {df['date'].nunique()}")
    print(f"Fichier créé : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
