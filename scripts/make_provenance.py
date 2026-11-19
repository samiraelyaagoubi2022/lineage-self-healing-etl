import pandas as pd, hashlib, os, datetime

# (dataset, index de la ressource dans valides.csv, version)
CHOIX = [
    ("liste-des-etablissements-publics-primaire",   1, "2023"),
    ("liste-des-etablissements-publics-primaire",   0, "2026"),
    ("liste-de-etablissements-publics-collegial",   1, "2023"),
    ("liste-de-etablissements-publics-collegial",   0, "2026"),
    ("liste-des-etablissements-publics-qualifiants", 1, "2023"),
    ("liste-des-etablissements-publics-qualifiants", 0, "2026"),
    ("liste-des-etablissements-prives-2023",        1, "2023"),
    ("liste-des-etablissements-prives-2023",        0, "2026"),
    ("la-liste-des-hopitaux",                       0, "2025"),
]
v = pd.read_csv("valides.csv")
lines = ["| Jeu | Version | URL | Téléchargé le | sha256 |", "|---|---|---|---|---|"]
for name, k, ver in CHOIX:
    url = v[v.dataset == name].iloc[k]["url"]
    path = f"data/raw/{name}_{k}.xlsx"
    sha = hashlib.sha256(open(path, "rb").read()).hexdigest()
    day = datetime.date.fromtimestamp(os.path.getmtime(path))
    lines.append(f"| {name} | {ver} | {url} | {day} | `{sha}` |")
open("provenance.md", "w", encoding="utf-8").write("\n".join(lines))
print("provenance.md écrit")