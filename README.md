# Statcast → Snowflake Pipeline

An Airflow data pipeline that ingests MLB Statcast pitch-level data into Snowflake daily. Built as a portfolio project demonstrating production data engineering patterns: incremental loading, data quality validation, idempotency, containerization, and cloud data warehousing.

## What it does

- Pulls pitch-by-pitch Statcast data from Baseball Savant via `pybaseball`
- Pulls team batting and pitching game logs from Baseball Reference via `pybaseball`
- Cleans and validates the data (column selection, type coercion, quality checks)
- Loads into Snowflake using bulk `PUT/COPY` (fast, not row-by-row inserts)
- Runs daily on a schedule; supports manual backfills by season or date range
- Exposes aggregated views (pitcher arsenal, xwOBA by pitch type) for analysts

## Architecture

```
Baseball Savant API / Baseball Reference
       │
       ▼
  [pybaseball]          ← Python library wrapping the Statcast endpoint
       │
       ▼
  Airflow DAG           ← Orchestration (scheduling, retries, monitoring)
  ┌────────────────────────────────────────────┐
  │  get_date_range → check_already_loaded     │
  │       → extract → transform_and_validate   │
  │       → load_to_snowflake → refresh_views  │
  └────────────────────────────────────────────┘
       │
       ▼
    Snowflake            ← Cloud data warehouse
    BASEBALL.STATCAST.RAW_PITCHES   (raw layer - pitch-level Statcast)
    BASEBALL.STATCAST.RAW_BATTERS    (raw layer - batter Statcast)
    BASEBALL.HISTORICAL.TEAM_BATTING_LOGS   (team batting game logs)
    BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS  (team pitching game logs)
```

## Project structure

```
statcast-pipeline/
├── docker-compose.yml          # Airflow + Postgres (metadata DB)
├── .env.example                # Environment variable template
├── dags/
│   ├── statcast_pitcher_backfill.py    # Historical Statcast pitcher data backfill
│   ├── statcast_batter_backfill.py    # Historical Statcast batter data backfill
│   ├── historical_team_batting_logs_backfill.py   # Team batting game logs backfill
│   ├── historical_team_pitching_logs_backfill.py  # Team pitching game logs backfill
│   └── utils/
│       ├── snowflake_utils.py  # Snowflake connection helpers
│       └── transform_utils.py  # Cleaning + validation logic
├── helpers/
│   └── historical_team_helpers.py  # Helper functions for team data
├── include/
│   └── sql/
│       ├── setup_snowflake_pitcher.sql      # Pitcher table setup
│       ├── setup_snowflake_batter.sql       # Batter table setup
│       └── setup_snowflake_team_game_logs.sql  # Team game logs table setup
└── tests/
    └── test_transform_utils.py # Unit tests (no Airflow/Snowflake needed)
```

## Setup

### Prerequisites
- Docker + Docker Compose
- A Snowflake account ([free 30-day trial](https://trial.snowflake.com) — $400 credits, more than enough)

### 1. Clone and configure

```bash
git clone <your-repo>
cd statcast-pipeline
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 2. Set up Snowflake

Run the SQL setup scripts in your Snowflake worksheet:

**For Statcast pitcher data:**
- Run `include/sql/setup_snowflake_pitcher.sql`

**For Statcast batter data:**
- Run `include/sql/setup_snowflake_batter.sql`

**For historical team game logs:**
- Run `include/sql/setup_snowflake_team_game_logs.sql`

This creates:
- The `BASEBALL` database with `STATCAST` and `HISTORICAL` schemas
- `BASEBALL.STATCAST.RAW_PITCHES` - Pitch-level Statcast data
- `BASEBALL.STATCAST.RAW_BATTERS` - Batter Statcast data
- `BASEBALL.HISTORICAL.TEAM_BATTING_LOGS` - Team batting game logs
- `BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS` - Team pitching game logs

### 3. Start Airflow

```bash
# Initialize the database and create admin user
docker compose up airflow-init

# Start all services
docker compose up -d

# Airflow UI → http://localhost:8080 (admin / admin)
```

### 4. Add the Snowflake connection in Airflow

Admin → Connections → Add:
- **Conn Id:** `snowflake_default`
- **Conn Type:** Snowflake
- **Schema:** STATCAST
- **Login:** your Snowflake username
- **Password:** your Snowflake password
- **Extra:** `{"account": "xy12345.us-east-1", "warehouse": "COMPUTE_WH", "database": "BASEBALL", "role": "SYSADMIN"}`

### 5. Run the tests

```bash
pip install pybaseball pandas pytest
pytest tests/ -v
```

### 6. Backfill data

**Statcast pitcher data:**
Trigger `statcast_pitcher_backfill` from the Airflow UI with config:
```json
{ "season": 2023 }
```

**Statcast batter data:**
Trigger `statcast_batter_backfill` from the Airflow UI with config:
```json
{ "season": 2023 }
```

**Historical team batting logs:**
Trigger `historical_team_batting_logs_backfill` from the Airflow UI with config:
```json
{ "season": 2023 }
```

**Historical team pitching logs:**
Trigger `historical_team_pitching_logs_backfill` from the Airflow UI with config:
```json
{ "season": 2023 }
```

## Key design decisions

**Incremental loading with idempotency**  
The pipeline checks `MAX(game_date)` before each run. If the target date is already loaded, it skips gracefully. This means you can re-run any DAG run without creating duplicates.

**Validation before loading**  
Data quality checks run *before* data hits Snowflake. If pitch speeds are impossibly low or launch angles are out of range, the task fails loudly rather than silently corrupting the warehouse. In production, this would trigger a PagerDuty alert.

**Separation of concerns**  
Transforms live in `transform_utils.py`, not in the DAG. This makes them independently testable — you can run `pytest` without Airflow or Snowflake running at all.

**TaskFlow API**  
Uses Airflow's modern `@task` decorator style rather than the older `PythonOperator`. This makes data flow between tasks explicit (function arguments) rather than hidden in XCom.push/pull calls.

**Dynamic task mapping (backfill DAG)**  
`extract_chunk.expand(chunk=chunks)` spawns one Airflow task per weekly chunk automatically. This is Airflow 2.3+ dynamic task mapping — it's how you parallelize work without hardcoding task counts.

## Querying the data

Once loaded, open a Snowflake worksheet:

```sql
-- Gerrit Cole's pitch mix in 2023
SELECT pitch_name, pitches_thrown, avg_velo, avg_spin, avg_xwoba
FROM BASEBALL.STATCAST.PITCHER_ARSENAL
WHERE game_year = 2023 AND player_name = 'Cole, Gerrit'
ORDER BY pitches_thrown DESC;

-- All pitches 99+ mph in 2023
SELECT player_name, game_date, release_speed, pitch_name, events
FROM BASEBALL.STATCAST.PITCHES
WHERE game_year = 2023 AND release_speed >= 99
ORDER BY release_speed DESC;

-- Team batting stats for a specific season
SELECT Team, Season, SUM(R) as total_runs, SUM(HR) as total_hr, SUM(SO) as total_so
FROM BASEBALL.HISTORICAL.TEAM_BATTING_LOGS
WHERE Season = 2023
GROUP BY Team, Season
ORDER BY total_runs DESC;

-- Team pitching ERA leaders
SELECT Team, Season, ROUND(AVG(ERA), 2) as avg_era, SUM(SO) as total_strikeouts
FROM BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS
WHERE Season = 2023
GROUP BY Team, Season
ORDER BY avg_era ASC;
```

## Extending this project

Ideas for making this more impressive as a portfolio piece:

- **Add dbt** — Replace the raw SQL views with dbt models. Add a `dbt run` step at the end of the DAG.
- **Add Airflow alerting** — Set `email_on_failure=True` or integrate with Slack via `SlackWebhookOperator`.
- **Add a Streamlit dashboard** — Connect to Snowflake and visualize pitch movement, strike zone plots, etc.
- **Add pitcher similarity model** — Use the arsenal data to cluster pitchers by stuff profile. Load model outputs back to Snowflake.
- **Upgrade orchestration** — Swap Airflow for Prefect or Dagster to compare the developer experience.
