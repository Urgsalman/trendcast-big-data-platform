from pathlib import Path

ids_path = Path("wikimedia_hdfs_ids.txt")

event_ids = {
    line.strip()
    for line in ids_path.read_text(encoding="utf-8-sig").splitlines()
    if line.strip()
}

pages = {
    event_id.split(":", 2)[1]
    for event_id in event_ids
    if event_id.count(":") >= 2
}

print(f"Nombre de pages déjà collectées : {len(pages)}")
print("\nPages existantes :")

for page in sorted(pages):
    print(page)