import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS = "projet"

cibles = [
    ("jobs", "job_test"),
    ("datasets", "source_brute"),
    ("datasets", "table_finale"),
]
for genre, nom in cibles:
    r = requests.delete(f"{B}/namespaces/{NS}/{genre}/{quote(nom, safe='')}")
    print(f"DELETE {genre}/{nom} -> {r.status_code}")