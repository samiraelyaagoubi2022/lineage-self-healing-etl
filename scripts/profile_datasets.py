import pandas as pd, requests, io, os, datetime, unicodedata, warnings
warnings.filterwarnings("ignore")
import pandas as pd, glob, warnings
warnings.filterwarnings("ignore")

for f in sorted(glob.glob("data/raw/*_0.xlsx")):
    raw = pd.read_excel(f, header=None, engine="openpyxl", nrows=6)
    print("\n====", f)
    print(raw.to_string(max_colwidth=30))

CHOIX = [
    "liste-des-etablissements-publics-primaire",
    "liste-de-etablissements-publics-collegial",
    "liste-des-etablissements-publics-qualifiants",
    "liste-des-etablissements-prives-2023",
    "la-liste-des-hopitaux",
]
HEADERS = {"User-Agent": "Mozilla/5.0"}
os.makedirs("data/raw", exist_ok=True)

def norm(s):
    s = unicodedata.normalize("NFD", str(s).lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")

def find_header(raw):
    """Première ligne (sur 10) qui contient 'region'."""
    for i in range(min(10, len(raw))):
        if any("region" in norm(v) for v in raw.iloc[i].tolist()):
            return i
    return None

v = pd.read_csv("valides.csv")
for name in CHOIX:
    subset = v[v.dataset == name]
    print(f"\n==== {name} : {len(subset)} ressource(s) dans valides.csv")
    for k, (_, r) in enumerate(subset.iterrows()):
        try:
            resp = requests.get(r["url"], headers=HEADERS, timeout=120)
            path = f"data/raw/{name}_{k}.xlsx"
            open(path, "wb").write(resp.content)
            xl = pd.ExcelFile(path, engine="openpyxl")
            print(f"-- [{k}] {r['url']}")
            print("   feuilles :", xl.sheet_names)
            for sh in xl.sheet_names:
                raw = xl.parse(sh, header=None)
                h = find_header(raw)
                if h is None:
                    print(f"   feuille '{sh}' : {len(raw)} lignes, pas de colonne région trouvée")
                    continue
                d = xl.parse(sh, header=h)
                col = next(c for c in d.columns if "region" in norm(c))
                vals = sorted(d[col].dropna().astype(str).str.strip().unique())
                print(f"   feuille '{sh}' : en-tête ligne {h}, {len(d)} lignes, téléchargé le {datetime.date.today()}")
                print("   colonnes :", list(d.columns))
                print(f"   {col} : {len(vals)} valeurs distinctes")
                print("   ", vals)
        except Exception as e:
            print(f"-- [{k}] ERREUR : {str(e)[:120]}")