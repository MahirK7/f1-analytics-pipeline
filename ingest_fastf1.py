from __future__ import annotations

import os
from datetime import datetime, timedelta
import pandas as pd
import fastf1
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------- Config -------------
CACHE_DIR = "/opt/airflow/fastf1_cache"
SESSION_TYPE = "R"  # R=Race, Q=Quali, SQ, FP1, FP2, FP3

MYSQL_URI = os.getenv(
    "F1_MYSQL_URI",
    "mysql+pymysql://fluser:flpass@f1-mysql:3306/f1_strategy"
)

# ---------- Helpers ----------
def to_ms(val):
    if pd.isna(val):
        return None
    try:
        td = pd.to_timedelta(val)
        return int(td.total_seconds() * 1000)
    except Exception:
        try:
            return int(float(val) * 1000)
        except Exception:
            return None

def get_latest_race(year: int):
    """Return the latest completed race name for given season."""
    schedule = fastf1.get_event_schedule(year)
    today = datetime.utcnow().date()
    past_races = schedule[schedule['EventDate'].dt.date <= today]
    if past_races.empty:
        raise ValueError("No races completed yet this year")
    latest = past_races.iloc[-1]
    return latest['EventName'], latest['EventDate']

# ---------- Tasks ----------
def init_mysql():
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
        conn.execute(text("DROP TABLE IF EXISTS laps;"))
        conn.execute(text("DROP TABLE IF EXISTS drivers;"))
        conn.execute(text("DROP TABLE IF EXISTS sessions;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

        conn.execute(text("""
            CREATE TABLE sessions (
                session_id   INT AUTO_INCREMENT PRIMARY KEY,
                year         INT NOT NULL,
                track        VARCHAR(150) NOT NULL,
                session_type VARCHAR(50) NOT NULL,
                total_laps   INT NULL,
                weather      JSON NULL,
                safety_car_count INT DEFAULT 0,
                UNIQUE KEY uq_session (year, track, session_type)
            ) ENGINE=InnoDB;
        """))
        conn.execute(text("""
            CREATE TABLE drivers (
                driver_id   INT AUTO_INCREMENT PRIMARY KEY,
                driver_code VARCHAR(5) NOT NULL UNIQUE,
                full_name   VARCHAR(100) NOT NULL,
                team        VARCHAR(100) NULL
            ) ENGINE=InnoDB;
        """))
        conn.execute(text("""
            CREATE TABLE laps (
                lap_id              INT AUTO_INCREMENT PRIMARY KEY,
                session_id          INT NOT NULL,
                driver_id           INT NOT NULL,
                lap_number          INT,
                stint_id            INT,
                compound            VARCHAR(10),
                lap_time_ms         INT,
                sector1_ms          INT,
                sector2_ms          INT,
                sector3_ms          INT,
                position            INT,
                gap_to_leader_ms    INT,
                interval_to_next_ms INT,
                FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                FOREIGN KEY (driver_id)  REFERENCES drivers(driver_id)  ON DELETE CASCADE,
                UNIQUE KEY uq_lap (session_id, driver_id, lap_number)
            ) ENGINE=InnoDB;
        """))

def wait_for_fastf1(year: int, gp_name: str, session_type: str):
    """Fails if data not yet published → Airflow will retry later."""
    fastf1.Cache.enable_cache(CACHE_DIR)
    ses = fastf1.get_session(year, gp_name, session_type)
    ses.load()   # if not available, this raises
    print(f"✅ Data available for {gp_name} {year} {session_type}")

def ingest(year: int, gp_name: str, session_type: str):
    os.makedirs(CACHE_DIR, exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)

    print(f"Loading {year} {gp_name} {session_type} …")
    ses = fastf1.get_session(year, gp_name, session_type)
    ses.load(laps=True, telemetry=False, weather=True, messages=True)

    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO sessions (year, track, session_type, total_laps, weather, safety_car_count)
                VALUES (:y, :t, :s, :tl, :w, 0)
                ON DUPLICATE KEY UPDATE total_laps=VALUES(total_laps)
            """),
            dict(y=year, t=ses.event.OfficialEventName or gp_name,
                 s=ses.name, tl=int(getattr(ses, "total_laps", 0) or 0),
                 w="{}"),
        )
        sid = conn.execute(
            text("SELECT session_id FROM sessions WHERE year=:y AND track=:t AND session_type=:s"),
            dict(y=year, t=ses.event.OfficialEventName or gp_name, s=ses.name)
        ).scalar()
        print(f"Using session_id={sid}")

        # --- rest of ingest code unchanged (drivers + laps insert) ---
        if ses.results is not None and len(ses.results):
            for _, r in ses.results.iterrows():
                code = str(r.get("Abbreviation") or "").strip()
                name = str(r.get("FullName") or code).strip()
                team = str(r.get("TeamName") or "").strip()
                conn.execute(
                    text("""INSERT INTO drivers (driver_code, full_name, team)
                            VALUES (:c,:n,:t)
                            ON DUPLICATE KEY UPDATE full_name=VALUES(full_name), team=VALUES(team)"""),
                    dict(c=code, n=name, t=team)
                )

        codes = {row[0]: row[1] for row in conn.execute(text("SELECT driver_code, driver_id FROM drivers")).fetchall()}

        laps = ses.laps if (ses.laps is not None and not ses.laps.empty) else pd.DataFrame()
        print(f"Found {len(laps)} laps in session {sid}")

        vals = []
        for _, r in laps.iterrows():
            did = codes.get(str(r.get("Driver") or ""))
            if not did:
                continue
            vals.append(dict(
                session_id=sid,
                driver_id=did,
                lap_number=int(r.LapNumber) if pd.notna(r.LapNumber) else None,
                stint_id=int(r.Stint) if pd.notna(r.Stint) else None,
                compound=str(r.Compound) if pd.notna(r.Compound) else None,
                lap_time_ms=to_ms(r.LapTime),
                sector1_ms=to_ms(r.Sector1Time),
                sector2_ms=to_ms(r.Sector2Time),
                sector3_ms=to_ms(r.Sector3Time),
                position=int(r.Position) if pd.notna(r.Position) else None,
                gap_to_leader_ms=to_ms(r.get("GapToLeader")),
                interval_to_next_ms=to_ms(r.get("IntervalToPositionAhead")),
            ))

        if vals:
            conn.execute(text("""INSERT INTO laps (...) VALUES (...) ON DUPLICATE KEY UPDATE ..."""), vals)
            print(f"✅ {len(vals)} laps ingested for session {sid}")

def generate_strategy_chart(session_id: int = 1):
    # (unchanged plotting code from your version)
    ...

# ---------- DAG ----------
default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="fastf1_ingest_mysql",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # run daily, will skip if no new data
    catchup=False,
    default_args=default_args,
    tags=["f1", "mysql"],
) as dag:

    def latest_context(**kwargs):
        year = datetime.utcnow().year
        gp_name, date = get_latest_race(year)
        return dict(year=year, gp=gp_name, session_type=SESSION_TYPE)

    ctx = PythonOperator(
        task_id="get_latest_race",
        python_callable=latest_context,
        provide_context=True,
    )

    t1 = PythonOperator(task_id="init_mysql", python_callable=init_mysql)
    t_wait = PythonOperator(
        task_id="wait_for_fastf1",
        python_callable=wait_for_fastf1,
        op_kwargs={"year": "{{ ti.xcom_pull(task_ids='get_latest_race')['year'] }}",
                   "gp_name": "{{ ti.xcom_pull(task_ids='get_latest_race')['gp'] }}",
                   "session_type": SESSION_TYPE}
    )
    t2 = PythonOperator(
        task_id="ingest_fastf1",
        python_callable=ingest,
        op_kwargs={"year": "{{ ti.xcom_pull(task_ids='get_latest_race')['year'] }}",
                   "gp_name": "{{ ti.xcom_pull(task_ids='get_latest_race')['gp'] }}",
                   "session_type": SESSION_TYPE}
    )
    t3 = PythonOperator(task_id="generate_strategy_chart", python_callable=generate_strategy_chart)

    ctx >> t1 >> t_wait >> t2 >> t3
