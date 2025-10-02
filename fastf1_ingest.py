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
YEAR = 2024
GRAND_PRIX = "Bahrain"
SESSION = "R"  # R, Q, SQ, FP1, FP2, FP3

MYSQL_URI = os.getenv("F1_MYSQL_URI")  # injected by docker-compose


# ---------- Helpers ----------
def to_ms(val):
    """Convert pandas timedelta or float (s) to ms integer."""
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


# ---------- Tasks ----------
def init_mysql():
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id   INT AUTO_INCREMENT PRIMARY KEY,
                year         INT NOT NULL,
                track        VARCHAR(150) NOT NULL,
                session_type VARCHAR(20) NOT NULL,
                total_laps   INT NULL,
                weather      JSON NULL,
                safety_car_count INT DEFAULT 0,
                UNIQUE KEY uq_session (year, track, session_type)
            ) ENGINE=InnoDB;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS drivers (
                driver_id   INT AUTO_INCREMENT PRIMARY KEY,
                driver_code VARCHAR(5) NOT NULL UNIQUE,
                full_name   VARCHAR(100) NOT NULL,
                team        VARCHAR(100) NULL
            ) ENGINE=InnoDB;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS laps (
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


def ingest():
    os.makedirs(CACHE_DIR, exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)

    print(f"Loading {YEAR} {GRAND_PRIX} {SESSION} …")
    ses = fastf1.get_session(YEAR, GRAND_PRIX, SESSION)
    ses.load(laps=True, telemetry=False, weather=True, messages=True)

    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    with engine.begin() as conn:
        # upsert session
        conn.execute(
            text("""
                INSERT INTO sessions (year, track, session_type, total_laps, weather, safety_car_count)
                VALUES (:y, :t, :s, :tl, :w, 0)
                ON DUPLICATE KEY UPDATE total_laps=VALUES(total_laps)
            """),
            dict(y=YEAR, t=ses.event.OfficialEventName or GRAND_PRIX,
                 s=ses.name, tl=int(getattr(ses, "total_laps", 0) or 0),
                 w="{}"),
        )
        sid = conn.execute(
            text("SELECT session_id FROM sessions WHERE year=:y AND track=:t AND session_type=:s"),
            dict(y=YEAR, t=ses.event.OfficialEventName or GRAND_PRIX, s=ses.name)
        ).scalar()
        print(f"Using session_id={sid}")

        # upsert drivers
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
            print(f"Inserting/Updating {len(vals)} laps…")
            conn.execute(text("""
                INSERT INTO laps
                (session_id, driver_id, lap_number, stint_id, compound,
                 lap_time_ms, sector1_ms, sector2_ms, sector3_ms, position,
                 gap_to_leader_ms, interval_to_next_ms)
                VALUES
                (:session_id, :driver_id, :lap_number, :stint_id, :compound,
                 :lap_time_ms, :sector1_ms, :sector2_ms, :sector3_ms, :position,
                 :gap_to_leader_ms, :interval_to_next_ms)
                ON DUPLICATE KEY UPDATE
                    stint_id = VALUES(stint_id),
                    compound = VALUES(compound),
                    lap_time_ms = VALUES(lap_time_ms),
                    sector1_ms = VALUES(sector1_ms),
                    sector2_ms = VALUES(sector2_ms),
                    sector3_ms = VALUES(sector3_ms),
                    position = VALUES(position),
                    gap_to_leader_ms = VALUES(gap_to_leader_ms),
                    interval_to_next_ms = VALUES(interval_to_next_ms)
            """), vals)
        else:
            print("⚠️ No laps found to insert!")


def create_views():
    """Run SQL script with multiple statements safely."""
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    sql_file = "/opt/airflow/sql/create_views.sql"

    with open(sql_file, "r") as f:
        raw_sql = f.read()

    statements = [s.strip() for s in raw_sql.split(';') if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            print(f"Running: {stmt[:60]}...")  # show preview
            conn.execute(text(stmt))


# ---------- DAG ----------
default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fastf1_ingest_mysql",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # trigger manually
    catchup=False,
    default_args=default_args,
    tags=["f1", "mysql"],
) as dag:

    t1 = PythonOperator(
        task_id="init_mysql",
        python_callable=init_mysql
    )

    t2 = PythonOperator(
        task_id="ingest_fastf1",
        python_callable=ingest
    )

    t3 = PythonOperator(
        task_id="create_views",
        python_callable=create_views
    )

    # task dependencies
    t1 >> t2 >> t3
