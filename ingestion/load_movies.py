import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://de_user:de_pass@postgres:5432/movies")

with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS rw"))
    conn.commit()

df = pd.read_csv("/data/raw/movies.csv")

df.to_sql("raw_movies", engine, schema="rw", if_exists="append", index=False)

print("Movies loaded into rw.raw_movies")
