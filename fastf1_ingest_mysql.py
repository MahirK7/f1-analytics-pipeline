import os
import math
import pandas as pd
import fastf1
import mysql.connector as mysql

# ========= CONFIG =========
DB = dict(
    host="localhost",      # you are running the script on your machine
    port=3306,
    user="f1app",
    password="f1app",
    database="f1_strategy"
)

YEAR = 2024
GRAND_PRIX = "Saudi Arabia"     # e.g. "Bahrain", "Spain", "Monaco"
SESSION = "R"              # "R","Q","SQ","FP1","FP2","FP3"

CACHE_DIR = "fastf1_cache_dir"
# ==========================

os.makedirs(CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)

def to_ms(val):
    """Convert FastF1 time-like values to integer milliseconds or None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        # Timedelta or parseable to Timedelta
        td = pd.to_timedelta(val)
        return int(round(td.total_seconds() * 1000))
    except Exception:
        return None

def main():
    # Connect
    conn = mysql.connect(**DB)
    cur = conn.cursor(dictionary=True)
    print("Connected to MySQL:", DB["database"])

    print(f"Loading FastF1 {YEAR} {GRAND_PRIX} {SESSION} …")
    ses = fastf1.get_session(YEAR, GRAND_PRIX, SESSION)
    ses.load(laps=True, telemetry=False, weather=False, messages=False)

    # ---------- Upsert session & fetch session_id ----------
    # Unique key is (year, track, session_type)
    track_name = ses.event.OfficialEventName or GRAND_PRIX
    total_laps = int(getattr(ses, "total_laps", 0) or 0)

    cur.execute("""
        INSERT INTO sessions (year, track, session_type, total_laps, weather, safety_car_count)
        VALUES (%s, %s, %s, %s, JSON_OBJECT(), 0)
        ON DUPLICATE KEY UPDATE session_id=LAST_INSERT_ID(session_id),
                                total_laps=VALUES(total_laps)
    """, (YEAR, track_name, ses.name, total_laps))
    conn.commit()

    cur.execute("SELECT LAST_INSERT_ID() AS id")
    session_id = cur.fetchone()["id"]
    print("✅ session_id =", session_id)

    # ---------- Teams & Drivers ----------
    results = ses.results
    if results is not None and len(results):
        print(f"Found {len(results)} drivers in results")

        # Insert teams (if available)
        for _, r in results.iterrows():
            team = str(r.get("TeamName") or r.get("Team") or "").strip()
            if team:
                cur.execute("INSERT IGNORE INTO teams (team_name) VALUES (%s)", (team,))

        # Insert drivers
        for _, r in results.iterrows():
            code = str(r.get("Abbreviation") or "").strip()
            name = str(r.get("FullName") or code).strip()
            team = str(r.get("TeamName") or "").strip()
            if code:
                cur.execute("""
                    INSERT INTO drivers (driver_code, full_name, team)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE full_name=VALUES(full_name), team=VALUES(team)
                """, (code, name, team))
        conn.commit()

    # Map driver_code -> driver_id
    cur.execute("SELECT driver_id, driver_code FROM drivers")
    did_map = {row["driver_code"]: row["driver_id"] for row in cur.fetchall()}

    # ---------- Laps ----------
    laps = ses.laps if (ses.laps is not None and not ses.laps.empty) else pd.DataFrame()


    print(f"FastF1 returned {0 if laps is None else len(laps)} laps")
    if laps is not None and len(laps):
        laps = laps.copy()
        # for fallback gap calculations:
        laps["CumRaceMs"] = laps.groupby("Driver")["LapTime"].cumsum().apply(to_ms)

        lap_rows = []
        for _, row in laps.iterrows():
            code = row.get("Driver")
            driver_id = did_map.get(code)
            if not driver_id:
                continue

            # Prefer provided gaps; otherwise compute from cumulative race time
            gap_ms = None
            interval_ms = None

            if "GapToLeader" in laps.columns and pd.notna(row.get("GapToLeader")):
                gap_ms = to_ms(row["GapToLeader"])
            else:
                my_ms = row.get("CumRaceMs")
                same_lap = laps[(laps["LapNumber"] == row["LapNumber"]) & (laps["Position"] == 1)]
                if not same_lap.empty and pd.notna(my_ms):
                    leader_ms = same_lap["CumRaceMs"].iloc[0]
                    if pd.notna(leader_ms):
                        gap_ms = int(my_ms - leader_ms)

            if "IntervalToPositionAhead" in laps.columns and pd.notna(row.get("IntervalToPositionAhead")):
                interval_ms = to_ms(row["IntervalToPositionAhead"])
            else:
                my_ms = row.get("CumRaceMs")
                ahead = laps[(laps["LapNumber"] == row["LapNumber"]) & (laps["Position"] == row["Position"] - 1)]
                if not ahead.empty and pd.notna(my_ms):
                    ahead_ms = ahead["CumRaceMs"].iloc[0]
                    if pd.notna(ahead_ms):
                        interval_ms = int(my_ms - ahead_ms)

            lap_rows.append((
                session_id,
                driver_id,
                int(row["LapNumber"]),
                int(row["Stint"]) if pd.notna(row.get("Stint")) else None,
                str(row.get("Compound")) if pd.notna(row.get("Compound")) else None,
                to_ms(row.get("LapTime")),
                to_ms(row.get("Sector1Time")),
                to_ms(row.get("Sector2Time")),
                to_ms(row.get("Sector3Time")),
                int(row.get("Position")) if pd.notna(row.get("Position")) else None,
                gap_ms,
                interval_ms
            ))

        # Bulk insert
        cur.executemany("""
            INSERT INTO laps (
              session_id, driver_id, lap_number, stint_id, compound,
              lap_time_ms, sector1_ms, sector2_ms, sector3_ms, position,
              gap_to_leader_ms, interval_to_next_ms
            )
            VALUES (
              %s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,
              %s,%s
            )
            ON DUPLICATE KEY UPDATE
              stint_id=VALUES(stint_id),
              compound=VALUES(compound),
              lap_time_ms=VALUES(lap_time_ms),
              sector1_ms=VALUES(sector1_ms),
              sector2_ms=VALUES(sector2_ms),
              sector3_ms=VALUES(sector3_ms),
              position=VALUES(position),
              gap_to_leader_ms=VALUES(gap_to_leader_ms),
              interval_to_next_ms=VALUES(interval_to_next_ms)
        """, lap_rows)
        conn.commit()
        print(f"✅ Inserted/updated {len(lap_rows)} laps")

    # ---------- Pit stops (basic heuristic from PitIn/PitOut) ----------
    if laps is not None and len(laps):
        pits = laps[laps["PitInTime"].notna()].copy()
        pit_rows = []
        for _, r in pits.iterrows():
            code = r.get("Driver")
            driver_id = did_map.get(code)
            if not driver_id:
                continue
            lapnum = int(r["LapNumber"])
            comp_in = str(r.get("Compound")) if pd.notna(r.get("Compound")) else None

            next_lap = laps[(laps["Driver"] == code) & (laps["LapNumber"] == lapnum + 1)]
            comp_out = None
            if not next_lap.empty:
                v = next_lap["Compound"].iloc[0]
                if pd.notna(v):
                    comp_out = str(v)

            pit_in = r.get("PitInTime")
            pit_out = r.get("PitOutTime")
            if pd.isna(pit_out) and not next_lap.empty and "PitOutTime" in next_lap.columns:
                pit_out = next_lap["PitOutTime"].iloc[0]

            stop_ms = None
            if pd.notna(pit_in) and pd.notna(pit_out):
                stop_ms = to_ms(pd.to_timedelta(pit_out) - pd.to_timedelta(pit_in))

            pit_rows.append((session_id, driver_id, lapnum, comp_in, comp_out, stop_ms))

        if pit_rows:
            cur.executemany("""
                INSERT INTO pitstops (session_id, driver_id, lap_number, compound_in, compound_out, stop_time_ms)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  compound_in=VALUES(compound_in),
                  compound_out=VALUES(compound_out),
                  stop_time_ms=VALUES(stop_time_ms)
            """, pit_rows)
            conn.commit()
            print(f"✅ Inserted/updated {len(pit_rows)} pitstops")

    cur.close()
    conn.close()
    print("🎉 Ingest complete.")

if __name__ == "__main__":
    main()
