import sqlite3, pandas as pd

con = sqlite3.connect("drivedash.db")
print(con.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall())

df = pd.read_sql("SELECT * FROM trip_kpis LIMIT 5;", con)
print(df)
con.close()