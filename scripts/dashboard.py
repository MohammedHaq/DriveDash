import streamlit as st, pandas as pd
from sqlalchemy import create_engine
import joblib, pathlib

st.set_page_config(page_title="DriveDash Lite", layout="wide")
st.title("🚗 DriveDash Lite – Fleet Snapshot")

DB_URI = "sqlite:///drivedash.db"
engine = create_engine(DB_URI)
trip = pd.read_sql("SELECT * FROM trip_kpis", engine)

# --- KPI Cards ---
c1, c2, c3 = st.columns(3)
c1.metric("Trips", len(trip))
c2.metric("Avg records per trip", f"{trip['total_records'].mean():.1f}")
c3.metric("Pct active trips", f"{(trip['ignition_records'] >= 50).mean():.1%}")

# --- Detail table ---
veh = st.selectbox("Filter by device", sorted(trip["device_id"].unique()))
filtered = trip[trip["device_id"] == veh]
st.dataframe(filtered)

# --- One-click risk prediction ---
model = joblib.load(pathlib.Path("models/active_trip_lr.pkl"))
if st.button("Predict active status on ALL trips"):
    filtered["active_pred"] = model.predict(filtered[["total_records"]])
    st.dataframe(filtered[["trip_date", "total_records", "active_pred"]])
