from airflow.decorators import dag, task
from datetime import datetime
import matplotlib
matplotlib.use("Agg")  # ensures no GUI backend needed

@dag(
    schedule_interval="@once",   # run manually or once
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["f1", "strategy"],
)
def f1_strategy_chart():
    
    @task()
    def generate_chart():
        import pandas as pd
        import matplotlib.pyplot as plt
        from sqlalchemy import create_engine
        import matplotlib.patches as mpatches

        # --- DB Connection ---
        user = "fluser"
        password = "flpass"
        host = "localhost"
        port = "3306"
        database = "f1_strategy"
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

        # Choose session_id (can parameterize later)
        SESSION_ID = 1

        # --- Session info ---
        session_query = f"""
        SELECT track, year, session_type, total_laps
        FROM sessions
        WHERE session_id = {SESSION_ID};
        """
        session_info = pd.read_sql(session_query, engine).iloc[0]
        track_name = session_info["track"]
        year = session_info["year"]
        session_type = session_info["session_type"]
        total_laps = session_info["total_laps"]

        # --- Stint summary ---
        query = f"""
        SELECT
          full_name,
          compound,
          stint_start_lap,
          stint_end_lap
        FROM stint_summary
        WHERE session_id = {SESSION_ID}
        ORDER BY full_name, stint_start_lap;
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            print(f"No data found for session_id={SESSION_ID}")
            return

        # FIA colours
        compound_colors = {
            "HARD": "#D9D9D9",
            "MEDIUM": "yellow",
            "SOFT": "red",
            "INTERMEDIATE": "green",
            "WET": "blue"
        }

        # Pit stop count
        total_pitstops = df[df["stint_end_lap"] < total_laps].shape[0]

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(16,12))
        drivers = df["full_name"].unique()[::-1]

        for i, driver in enumerate(drivers):
            stints = df[df["full_name"] == driver]
            for _, stint in stints.iterrows():
                ax.broken_barh(
                    [(stint["stint_start_lap"], stint["stint_end_lap"] - stint["stint_start_lap"])],
                    (i-0.3, 0.6),
                    facecolors=compound_colors.get(stint["compound"], "grey")
                )
                pit_lap = stint["stint_end_lap"]
                if pit_lap < total_laps:
                    ax.plot(pit_lap, i, "o", color="black", markersize=12)
                    ax.text(pit_lap, i+0.35, str(pit_lap),
                            fontsize=9, ha="center", va="bottom", fontweight="bold")

        # Final lap annotation
        ax.text(total_laps, -1.5, f"Lap {total_laps}",
                fontsize=12, ha="center", va="top", fontweight="bold")

        ax.set_yticks(range(len(drivers)))
        ax.set_yticklabels(drivers, fontsize=10, fontweight="bold")
        ax.set_xlabel("Lap Number", fontsize=12)

        ax.set_title(f"{track_name} – Pit Stop Strategy", fontsize=16, fontweight="bold")
        plt.suptitle(f"{year} {session_type}", fontsize=12, style="italic", y=0.92)
        plt.grid(axis="x", linestyle="--", alpha=0.4)

        # Pit stop count annotation
        plt.text(1.02, 0.5, f"TOTAL PIT STOPS\n{total_pitstops}",
                 transform=ax.transAxes, fontsize=18,
                 fontweight="bold", color="red", va="center")

        # Tyre legend
        patches = [
            mpatches.Patch(color="#D9D9D9", label="Hard"),
            mpatches.Patch(color="yellow", label="Medium"),
            mpatches.Patch(color="red", label="Soft"),
            mpatches.Patch(color="green", label="Intermediate"),
            mpatches.Patch(color="blue", label="Wet")
        ]
        ax.legend(handles=patches, title="Tyres", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=10)

        # Save chart to file (instead of plt.show)
        output_file = f"C:/Users/Mahir Kardame/OneDrive/Desktop/Mahir Kardame/Coding Projects/F1DB/output/strategy_chart_session{SESSION_ID}.png"
        plt.tight_layout()
        plt.savefig(output_file, bbox_inches="tight")
        plt.close()

        print(f"Chart saved: {output_file}")

    generate_chart()

dag = f1_strategy_chart()
