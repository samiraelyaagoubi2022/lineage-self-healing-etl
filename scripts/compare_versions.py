import pandas as pd, warnings
warnings.filterwarnings("ignore")

NAMES = [
    "liste-des-etablissements-publics-primaire",
    "liste-de-etablissements-publics-collegial",
    "liste-des-etablissements-publics-qualifiants",
    "liste-des-etablissements-prives-2023",
]
MAP = {"COMMUNE": "LL_COM", "PROVINCE": "LL_PROV", "REGION": "LL_REG"}

def clean(s):
    return set(s.dropna().astype(str).str.strip())

for n in NAMES:
    dfs = [pd.read_excel(f"data/raw/{n}_{k}.xlsx", engine="openpyxl") for k in (0, 1)]
    v1 = next(d for d in dfs if "REGION" in d.columns)   # schéma 2023
    v2 = next(d for d in dfs if "LL_REG" in d.columns)   # schéma 2026
    print(f"\n==== {n}")
    print("lignes v1 / v2 :", len(v1), "/", len(v2))
    print("colonnes supprimées :", sorted(set(v1.columns) - set(v2.columns)),
          "| ajoutées :", sorted(set(v2.columns) - set(v1.columns)))
    for old, new in MAP.items():
        a, b = clean(v1[old]), clean(v2[new])
        print(f"{old} -> {new} | distinctes {len(a)}/{len(b)} | communes {len(a & b)}"
              f" | seulement v1 {sorted(a - b)[:6]} | seulement v2 {sorted(b - a)[:6]}")
    k1 = clean(v1["NOM_ETABL"].str.lower())
    k2 = clean(v2["NOM_ETABL"].str.lower())
    print(f"établissements : communs {len(k1 & k2)}, retirés {len(k1 - k2)}, ajoutés {len(k2 - k1)}")