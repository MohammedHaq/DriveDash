import pandas as pd, pathlib
from sqlalchemy import create_engine

DB_URI = "sqlite:///drivedash.db"
RAW_CSV = pathlib.Path("data/Telematicsdata.csv")

def load_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_CSV)
    df.columns = df.columns.str.lower()                 # unify
    df["timestamp"] = pd.to_datetime(df["timemili"], unit="ms", utc=True)
    return df

def tidy_events(raw: pd.DataFrame) -> pd.DataFrame:
    out_rows = []
    for _, r in raw.iterrows():
        var = r["variable"]
        val = r["value"]

        if var == "POSITION":
            lat, lon, _ = map(float, val.split(","))
            out_rows.append({
                "device_id": r["deviceid"],
                "timestamp": r["timestamp"],
                "lat": lat,
                "lon": lon,
            })

        elif var == "IGNITION_STATUS":
            out_rows.append({
                "device_id": r["deviceid"],
                "timestamp": r["timestamp"],
                "ignition_on": int(val in {"1", "true", "on"})
            })
        # add more elif-blocks as you discover variables

    return pd.DataFrame(out_rows)

def write_sqlite(df: pd.DataFrame) -> None:
    engine = create_engine(DB_URI)
    df.to_sql("clean_events", engine, if_exists="replace", index=False)

    # Trip-level roll-up
    q = """
    SELECT
        device_id,
        DATE(timestamp)                    AS trip_date,
        MIN(timestamp)                     AS start_ts,
        MAX(timestamp)                     AS end_ts,
        SUM(COALESCE(ignition_on, 0))      AS ignition_records,
        COUNT(*)                           AS total_records
    FROM clean_events
    GROUP BY device_id, DATE(timestamp);
    """
    trip_kpi = pd.read_sql(q, engine)
    trip_kpi.to_sql("trip_kpis", engine, if_exists="replace", index=False)

if __name__ == "__main__":
    write_sqlite(tidy_events(load_raw()))
    print("✅ ETL finished — tables clean_events & trip_kpis written")
