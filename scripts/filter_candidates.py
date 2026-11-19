import pandas as pd, requests, io
import warnings

warnings.filterwarnings("ignore")

df = pd.read_csv("exploration_ckan.csv")
df["format"] = df["format"].fillna("").str.lower()
df = df[df["format"].isin(["csv", "xlsx", "xls"])].drop_duplicates("url")
print(f"{len(df)} ressources CSV/XLSX uniques")

rows = []
for _, r in df.iterrows():
    info = {"dataset": r["dataset"], "format": r["format"], "url": r["url"],
            "modifie": r["modifie"], "ok": False, "colonnes": "", "lignes_apercu": 0, "erreur": ""}
    try:
        resp = requests.get(r["url"], timeout=30)
        resp.raise_for_status()
        if r["format"] == "csv":
            for enc in ("utf-8", "latin-1"):
                try:
                    d = pd.read_csv(io.BytesIO(resp.content), encoding=enc,
                                    sep=None, engine="python", nrows=200)
                    info["encodage"] = enc
                    break
                except Exception as e:
                    info["erreur"] = str(e)[:80]
                    d = None
        else:
            d = pd.read_excel(io.BytesIO(resp.content), nrows=200, engine="openpyxl")
            try:
                d = pd.read_excel(io.BytesIO(resp.content), nrows=200, engine="openpyxl")
            except Exception:
                d = pd.read_excel(io.BytesIO(resp.content), nrows=200, engine="calamine")
        if d is not None:
            info.update(ok=True, colonnes=" | ".join(map(str, d.columns)),
                        lignes_apercu=len(d), erreur="")
    except Exception as e:
        info["erreur"] = str(e)[:80]
    rows.append(info)

out = pd.DataFrame(rows)
out.to_csv("candidats.csv", index=False, encoding="utf-8-sig")
print(out["ok"].value_counts())