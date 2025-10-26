import os
import datetime as dt
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pymongo import MongoClient

# ---- env ----
load_dotenv()
PG_URI    = os.getenv("PG_URI", "postgresql+psycopg2://postgres:password@127.0.0.1:5432/SmartCampusDB")
PG_SCHEMA = os.getenv("PG_SCHEMA", "campus")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB  = os.getenv("MONGO_DB", "SmartCampus")

# ---- helpers ----
def qualify_with_schema(sql: str, schema: str) -> str:
    """Replace {S}.table with <schema>.table"""
    return sql.replace("{S}.", f"{schema}.")

def get_pg_engine(uri: str):
    return create_engine(uri, pool_pre_ping=True, future=True)

def run_postgres_query(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

def get_mongo_client(uri: str):
    return MongoClient(uri, uuidRepresentation="standard")

def _flatten(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        nk = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten(v, nk, sep=sep).items())
        else:
            items.append((nk, v))
    return dict(items)

def run_mongo_aggregate(client, db_name: str, collection: str, pipeline: list) -> pd.DataFrame:
    cur = client[db_name][collection].aggregate(pipeline, allowDiskUse=True)
    rows = []
    for doc in cur:
        if "_id" in doc and isinstance(doc["_id"], dict):
            doc["_id"] = str(doc["_id"])
        rows.append(_flatten(doc))
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def render_chart(df: pd.DataFrame, chart_cfg: dict):
    if df is None or df.empty:
        st.info("No data.")
        return
    t = chart_cfg.get("type", "table")
    if t == "table":
        st.dataframe(df, use_container_width=True)
        return
    if t == "bar":
        fig = px.bar(df, x=chart_cfg["x"], y=chart_cfg["y"])
    elif t == "line":
        fig = px.line(df, x=chart_cfg["x"], y=chart_cfg["y"])
    elif t == "pie":
        fig = px.pie(df, names=chart_cfg["names"], values=chart_cfg["values"])
    else:
        st.dataframe(df, use_container_width=True)
        return
    st.plotly_chart(fig, use_container_width=True)

# ---- CONFIG (queries) ----
CONFIG = {
    "postgres": {
        "enabled": True,
        "queries": {
            
            "Top Stations by Visits (7d)": {
                "sql": """
                    WITH recent_stop AS (
                        SELECT station_id
                        FROM {S}.trip_stop
                        WHERE passed_ts >= NOW() - INTERVAL '7 days'
                    )
                    SELECT ds.station_name, COUNT(*) AS visits
                    FROM recent_stop rs
                    JOIN {S}.docking_station ds ON ds.station_id = rs.station_id
                    GROUP BY ds.station_name
                    ORDER BY visits DESC
                    LIMIT 10;
                """,
                "chart": {"type": "bar", "x": "station_name", "y": "visits"},
                "tags": ["stations","7d"]
            },
            
            "Avg Trip Distance per Day (14d)": {
                "sql": """
                    SELECT DATE(t.start_ts) AS day,
                           ROUND(AVG(br.distance_km)::numeric, 2) AS avg_km
                    FROM {S}.trip t
                    JOIN {S}.bus_route br ON br.route_id = t.route_id
                    WHERE t.start_ts >= NOW() - INTERVAL '14 days'
                    GROUP BY day
                    ORDER BY day;
                """,
                "chart": {"type": "line", "x": "day", "y": "avg_km"},
                "tags": ["trips","14d"]
            },
            "Vehicle Status Distribution": {
                "sql": """
                    SELECT status, COUNT(*) AS cnt
                    FROM {S}.vehicle
                    GROUP BY status
                    ORDER BY cnt DESC;
                """,
                "chart": {"type": "bar", "x": "status", "y": "cnt"},
                "tags": ["vehicles"]
            },
            "Low Battery Vehicles (<=20%)": {
                "sql": """
                    SELECT v.vehicle_id, v.model, v.battery_percent, ds.station_name
                    FROM {S}.vehicle v
                    LEFT JOIN {S}.docking_station ds ON ds.station_id = v.current_station_id
                    WHERE v.battery_percent <= 20
                    ORDER BY v.battery_percent ASC
                    LIMIT 50;
                """,
                "chart": {"type": "table"},
                "tags": ["vehicles","battery"]
            },
            "Bookings by Status (30d)": {
                "sql": """
                    SELECT status, COUNT(*) AS cnt
                    FROM {S}.booking
                    WHERE start_ts >= NOW() - INTERVAL '30 days'
                    GROUP BY status
                    ORDER BY cnt DESC;
                """,
                "chart": {"type": "bar", "x": "status", "y": "cnt"},
                "tags": ["booking","30d"]
            },
            "Top Routes by Trips": {
                "sql": """
                    SELECT br.route_name, COUNT(*) AS trips
                    FROM {S}.trip t
                    JOIN {S}.bus_route br ON br.route_id = t.route_id
                    GROUP BY br.route_name
                    ORDER BY trips DESC
                    LIMIT 10;
                """,
                "chart": {"type": "bar", "x": "route_name", "y": "trips"},
                "tags": ["routes"]
            }
        }
    },
    "mongo": {
        "enabled": True,
        "queries": {
            "Latest 20 Sensor Readings": {
                "collection": "sensor_readings_ts",
                "aggregate": [
                    {"$sort": {"ts": -1}},
                    {"$limit": 20},
                    {"$project": {"_id": 0, "ts": 1, "sensor_meta.sensor_id": 1, "temperature_c": 1, "humidity_pct": 1}}
                ],
                "chart": {"type": "table"}
            },
            "Sensor Readings per Hour (24h)": {
                "collection": "sensor_readings_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}}},
                    {"$project": {"hour": {"$dateTrunc": {"date": "$ts", "unit": "hour"}}}},
                    {"$group": {"_id": "$hour", "count": {"$count": {}}}},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "line", "x": "_id", "y": "count"}
            },
            "Avg Temperature by Hour (24h)": {
                "collection": "sensor_readings_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}, "temperature_c": {"$ne": None}}},
                    {"$group": {
                        "_id": {"$dateTrunc": {"date": "$ts", "unit": "hour"}},
                        "avgTemp": {"$avg": "$temperature_c"}
                    }},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "line", "x": "_id", "y": "avgTemp"}
            },
            "Vehicle Battery Buckets": {
                "collection": "realtime_availability",
                "aggregate": [
                    {"$match": {"battery_percent": {"$ne": None}}},
                    {"$bucket": {
                        "groupBy": "$battery_percent",
                        "boundaries": [0,20,40,60,80,100],
                        "default": "100+",
                        "output": {"count": {"$sum": 1}}
                    }},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "count"}
            },
            "Latest Vehicle Status (20)": {
                "collection": "realtime_availability",
                "aggregate": [
                    {"$sort": {"last_updated": -1}},
                    {"$limit": 20},
                    {"$project": {"_id": 0, "vehicle_id": 1, "status": 1, "battery_percent": 1, "last_updated": 1}}
                ],
                "chart": {"type": "table"}
            }
        }
    }
}

# ---- UI ----
def main():
    st.set_page_config(page_title="SmartCampus Dashboard", layout="wide")
    st.title("SmartCampus Dashboard")

    with st.sidebar:
        st.subheader("Connections")
        st.text_input("PG_URI", value=PG_URI, key="pg_uri")
        st.text_input("PG_SCHEMA", value=PG_SCHEMA, key="pg_schema")
        st.text_input("MONGO_URI", value=MONGO_URI, key="mongo_uri")
        st.text_input("MONGO_DB", value=MONGO_DB, key="mongo_db")
        auto_run = st.checkbox("Auto run", value=True)

    # ---- Postgres panel ----
    st.header("PostgreSQL")
    try:
        engine = get_pg_engine(st.session_state["pg_uri"])
        names = list(CONFIG["postgres"]["queries"].keys())
        sel = st.selectbox("Choose a SQL query", names, key="pg_sel")
        if sel:
            q = CONFIG["postgres"]["queries"][sel]
            sql = qualify_with_schema(q['sql'], st.session_state['pg_schema'])
            st.code(sql, language="sql")
            if auto_run or st.button("Run SQL"):
                df = run_postgres_query(engine, sql)
                render_chart(df, q["chart"])
    except Exception as e:
        st.error(f"Postgres error: {e}")

    # ---- Mongo panel ----
    st.header("MongoDB")
    try:
        client = get_mongo_client(st.session_state["mongo_uri"])
        names = list(CONFIG["mongo"]["queries"].keys())
        sel = st.selectbox("Choose a Mongo aggregation", names, key="mongo_sel")
        if sel:
            q = CONFIG["mongo"]["queries"][sel]
            st.write(f"Collection: `{q['collection']}`")
            st.code(str(q["aggregate"]), language="python")
            if auto_run or st.button("Run Mongo"):
                dfm = run_mongo_aggregate(client, st.session_state["mongo_db"], q["collection"], q["aggregate"])
                render_chart(dfm, q["chart"])
    except Exception as e:
        st.error(f"Mongo error: {e}")

if __name__ == "__main__":
    main()
