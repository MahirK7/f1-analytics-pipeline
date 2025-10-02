🏎️ F1 Data Engineering Pipeline

An end-to-end Formula 1 data engineering pipeline that ingests race telemetry from the FastF1 API
, cleans and structures it in MySQL, orchestrates workflows with Apache Airflow, and visualizes insights via Python plots, Grafana, and Power BI.

🚀 Features

ETL Pipeline – Extracts raw race/session data, transforms it (null handling, outlier removal, standardization), and loads into MySQL.

Workflow Orchestration – Airflow DAGs automate ingestion, retries, and scheduling.

Database Design – Normalized schema with sessions, drivers, and laps tables.

Containerized Deployment – Docker Compose runs Airflow, MySQL, and Grafana together.

Analytics & Visualization – Python (Matplotlib/Seaborn) for tyre degradation and stint analysis, plus dashboards in Grafana/Power BI.

CI/CD Practices – Version control with Git + Docker for reproducibility and scalable deployment.

🛠️ Tech Stack

Python (FastF1, Pandas, SQLAlchemy, Matplotlib, Seaborn)

MySQL (data storage & querying)

Apache Airflow (workflow orchestration)

Docker & Docker Compose (containerization)

Grafana & Power BI (dashboards & visualization)

Git (version control)

📂 Project Structure
f1-analytics-pipeline/
│── dags/                  # Airflow DAGs (ETL & ingestion scripts)
│── sql/                   # Table creation / SQL queries
│── analysis/              # Python analysis scripts (tyre degradation, lap analysis)
│── docker-compose.yml     # Orchestration for Airflow, MySQL, Grafana
│── requirements.txt       # Python dependencies
│── README.md              # Project documentation

⚡ Getting Started

Clone the repo

git clone https://github.com/yourusername/f1-analytics-pipeline.git
cd f1-analytics-pipeline


Start Docker services

docker-compose up -d


Access services

Airflow UI → http://localhost:8080

Grafana → http://localhost:3000

MySQL → localhost:3306

Trigger the DAG

In Airflow, trigger ingest_fastf1_dag to fetch and store race data.

📊 Example Insights

Average lap times per driver per compound

Tyre degradation trends across stints

Gap to leader comparisons

Interactive dashboards in Grafana / Power BI

🎯 Skills Demonstrated

ETL & Data Pipelines

Workflow Orchestration (Airflow)

Database Engineering (MySQL, SQL queries)

Data Preprocessing (null handling, outlier detection, standardization)

Containerization (Docker)

CI/CD Practices

Visualization & Dashboarding (Python, Grafana, Power BI)

🚦 Future Improvements

Automate multi-race ingestion

Real-time streaming with Kafka/Spark

Enrich dashboards with pit stop strategies & weather data

Cloud deployment (AWS/GCP/Azure)

📌 Author

👤 Mahir Kardame
🔗 LinkedIn
 | GitHub
