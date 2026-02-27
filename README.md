# Statcast Pipeline (Historical Team Logs)

An Airflow data pipeline that ingests historical MLB team batting and pitching game logs from Baseball Reference into Snowflake.

## What it does

- Pulls team batting and pitching game logs from Baseball Reference via web scraping
- Cleans and validates the data (column mapping, type coercion)
- Loads into Snowflake using bulk inserts
- Manual backfill DAGs triggered by team and season

## Architecture

```
Baseball Reference
       │
       ▼
   Web Scraping          ← BeautifulSoup + pandas
   (pybaseball)
       │
       ▼
   Airflow DAG           ← Orchestration (manual backfills)
   ┌────────────────────────────────────────┐
   │  get_teams → get_seasons               │
   │       → extract_team_batting/pitching  │
   │       → load_team_batting/pitching     │
   └────────────────────────────────────────┘
       │
       ▼
     Snowflake
     BASEBALL.HISTORICAL.TEAM_BATTING_LOGS
     BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS
```

## Project structure

```
statcast-pipeline/
├── docker-compose.yml          # Airflow + Postgres (metadata DB)
├── .env.example                # Environment variable template
├── dags/
│   ├── historical_team_batting_logs_backfill.py   # Team batting logs backfill
│   ├── historical_team_pitching_logs_backfill.py  # Team pitching logs backfill
│   └── utils/
│       ├── historical_team_utils.py  # Scraping + data processing
│       ├── snowflake_utils.py         # Snowflake connection helpers
│       ├── transform_utils.py         # Cleaning + validation logic
│       └── bref.py                     # Baseball Reference session
├── include/
│   └── sql/
│       └── setup_snowflake_team_game_logs.sql  # Team game logs table setup
└── tests/
    └── test_transform_utils.py # Unit tests
```

## Setup

### Prerequisites
- Docker + Docker Compose
- A Snowflake account

### 1. Clone and configure

```bash
git clone <your-repo>
cd statcast-pipeline
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 2. Set up Snowflake

Run `include/sql/setup_snowflake_team_game_logs.sql` in your Snowflake worksheet.

This creates:
- The `BASEBALL` database with `HISTORICAL` schema
- `BASEBALL.HISTORICAL.TEAM_BATTING_LOGS` - Team batting game logs
- `BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS` - Team pitching game logs

### 3. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
# Airflow UI → http://localhost:8080 (admin / admin)
```

### 4. Add the Snowflake connection in Airflow

Admin → Connections → Add:
- **Conn Id:** `snowflake_default`
- **Conn Type:** Snowflake
- **Schema:** HISTORICAL
- **Login:** your Snowflake username
- **Password:** your Snowflake password
- **Extra:** `{"account": "xy12345.us-east-1", "warehouse": "COMPUTE_WH", "database": "BASEBALL", "role": "SYSADMIN"}`

### 5. Run the tests

```bash
pip install pybaseball pandas pytest beautifulsoup4 lxml
pytest tests/ -v
```

### 6. Backfill data

**Team batting logs:**
Trigger `historical_team_batting_logs_backfill` from the Airflow UI with config:
```json
{
  "teams": ["NYY", "BOS"],
  "seasons": [2023, 2024]
}
```

**Team pitching logs:**
Trigger `historical_team_pitching_logs_backfill` from the Airflow UI with config:
```json
{
  "teams": ["NYY", "BOS"],
  "seasons": [2023, 2024]
}
```

## Key design decisions

**Dynamic task mapping**  
Uses Airflow 2.3+ dynamic task mapping to parallelize by team and season. Each team/season combination spawns its own task.

**Separation of concerns**  
Transforms live in `transform_utils.py`, not in the DAG. This makes them independently testable.

**TaskFlow API**  
Uses Airflow's modern `@task` decorator style rather than the older `PythonOperator`.

## Querying the data

```sql
-- Team batting stats for a specific season
SELECT Team, Season, SUM(R) as total_runs, SUM(HR) as total_hr
FROM BASEBALL.HISTORICAL.TEAM_BATTING_LOGS
WHERE Season = 2023
GROUP BY Team, Season
ORDER BY total_runs DESC;

-- Team pitching ERA leaders
SELECT Team, Season, ROUND(AVG(ERA), 2) as avg_era, SUM(SO) as total_so
FROM BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS
WHERE Season = 2023
GROUP BY Team, Season
ORDER BY avg_era ASC;
```

## Extending this project

- Add dbt for data modeling
- Add Airflow alerting (email/Slack on failure)
- Add a Streamlit dashboard for visualization
