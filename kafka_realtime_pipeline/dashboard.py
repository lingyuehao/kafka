import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Flight Status Dashboard", layout="wide")
st.title("✈️ Real-Time Flight Status Dashboard")
st.caption("Live updates of flight activities streamed through Kafka → PostgreSQL → Streamlit")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5435/kafka_db"


@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


engine = get_engine()


def load_data(limit: int = 500) -> pd.DataFrame:
    query = text(
        """
        SELECT *
        FROM flight_events
        ORDER BY event_time DESC
        LIMIT :limit
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"limit": limit})
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

st.sidebar.header("Controls")
refresh = st.sidebar.slider("Auto-refresh interval (seconds)", 2, 20, 5)
limit_rows = st.sidebar.number_input(
    "Number of recent events to load", min_value=50, max_value=2000, value=500, step=50
)
selected_airline = st.sidebar.selectbox("Filter by airline (optional)", ["All"])

placeholder = st.empty()

while True:
    df = load_data(limit=int(limit_rows))

    with placeholder.container():
        if df.empty:
            st.warning("No data yet. Make sure consumer & producer are running.")
            st.stop()
        for col in ["departure_time", "event_time"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        airlines = sorted(df["airline"].dropna().unique().tolist())

        total_flights = len(df)
        avg_delay = df["delay_minutes"].mean()
        on_time = (df["status"] == "On Time").sum()
        cancelled = (df["status"] == "Cancelled").sum()
        on_time_rate = on_time / total_flights * 100 if total_flights > 0 else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Events (rows)", total_flights)
        k2.metric("Avg Delay (min)", f"{avg_delay:.1f}")
        k3.metric("On-time Rate", f"{on_time_rate:.1f}%")
        k4.metric("Cancelled", cancelled)

        st.markdown("### Recent Flight Events")
        st.dataframe(df.head(20), use_container_width=True)

        delay_by_airline = (
            df.groupby("airline")["delay_minutes"]
            .mean()
            .reset_index()
            .sort_values("delay_minutes", ascending=False)
        )
        fig1 = px.bar(
            delay_by_airline,
            x="airline",
            y="delay_minutes",
            title="Average Delay by Airline (minutes)",
        )
        
        status_counts = df["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig2 = px.pie(
            status_counts,
            values="count",
            names="status",
            title="Flight Status Distribution",
        )

        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            st.plotly_chart(fig2, use_container_width=True)

        df_ts = (
            df.set_index("event_time")
            .resample("1min")["delay_minutes"]
            .mean()
            .reset_index()
        )
        fig3 = px.line(
            df_ts,
            x="event_time",
            y="delay_minutes",
            title="Average Delay Over Time (1-min buckets)",
        )
        st.markdown("### Delay Over Time")
        st.plotly_chart(fig3, use_container_width=True)

        st.caption(f"Last refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    time.sleep(refresh)
