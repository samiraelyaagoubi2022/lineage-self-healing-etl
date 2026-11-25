import glob
import duckdb

for db in glob.glob("/opt/airflow/warehouse/*.duckdb"):
    con = duckdb.connect(db, read_only=True)
    print("==", db)
    try:
        cols = [r[0] for r in con.execute("DESCRIBE raw._provenance").fetchall()]
        n = con.execute('SELECT count(*) FROM raw."_provenance"').fetchone()[0]
        print("   colonnes :", cols)
        print("   lignes   :", n)
    except Exception as e:
        print("   pas de raw._provenance :", str(e)[:120])
    con.close()