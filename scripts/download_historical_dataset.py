
from pathlib import Path

from datasets import load_dataset

DATASET_ID = "vtasca/wikipedia-pageviews"
OUTPUT_DIR = Path("data/historical")
OUTPUT_FILE = OUTPUT_DIR / "wikipedia_pageviews.parquet"

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Chargement du dataset : {DATASET_ID}")

    dataset = load_dataset(DATASET_ID, split="train")

    print(f"Nombre de lignes : {len(dataset)}")
    print(f"Colonnes : {dataset.column_names}")

    required_columns = {"rank", "article", "views", "date"}
    missing = required_columns - set(dataset.column_names)

    if missing:
        raise ValueError(f"Colonnes manquantes : {missing}")

    print("Aperçu des données :")
    print(dataset.select(range(min(5, len(dataset)))))

    dataset.to_parquet(str(OUTPUT_FILE))

    print(f"\nDataset sauvegardé : {OUTPUT_FILE}")
    print(f"Taille : {OUTPUT_FILE.stat().st_size / 1024 / 1024:.2f} Mo")

if __name__ == "__main__":
    main()