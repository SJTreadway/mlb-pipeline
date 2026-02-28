# MLB Pipeline (Historical Team Logs + Retrosheet Events)

An Airflow data pipeline that ingests historical MLB team batting and pitching game logs from Baseball Reference and game events data from Retrosheet into Snowflake.

## What it does

- **Baseball Reference**: Pulls team batting and pitching game logs from Baseball Reference via web scraping
- **Retrosheet Events**: Pulls game event data from Retrosheet (via pybaseball) including:
  - Full game box scores (batting, pitching, fielding stats for both teams)
  - Starting lineups with player IDs and positions
  - Manager and umpire information
  - Game metadata (attendance, duration, park, etc.)
  - Rolling window statistics (162/90/30 game rolling sums for BATAVG, OBP, SLG, OBS, SB, CS, ERR)
- Cleans and validates the data (column mapping, type coercion)
- Loads into Snowflake using bulk inserts
- Manual backfill DAGs triggered by season

## Architecture

```
Baseball Reference                     Retrosheet (pybaseball)
        │                                      │
        ▼                                      ▼
    Web Scraping                        GitHub Raw API
    (pybaseball)                        (chadwickbureau/retrosheet)
        │                                      │
        ▼                                      ▼
    Airflow DAG                           Airflow DAG
    ┌────────────────────────────────┐    ┌────────────────────────────────┐
    │  get_teams → get_seasons      │    │  get_seasons                   │
    │       → extract_team_batting   │    │       → extract_retrosheet     │
    │       → extract_team_pitching  │    │       → load_retrosheet_events │
    │       → load_team_batting      │    └────────────────────────────────┘
    │       → load_team_pitching     │
    └────────────────────────────────┘              │
        │                                           ▼
        ▼                                     Snowflake
      Snowflake                          BASEBALL.HISTORICAL.RETROSHEET_EVENTS
BASEBALL.HISTORICAL.TEAM_BATTING_LOGS
BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS
```

## Project structure

```
mlb-pipeline/
├── docker-compose.yml          # Airflow + Postgres (metadata DB)
├── .env.example                # Environment variable template
├── dags/
│   ├── historical_team_batting_logs_backfill.py   # Team batting logs backfill
│   ├── historical_team_pitching_logs_backfill.py  # Team pitching logs backfill
│   ├── historical_retrosheet_events_backfill.py   # Retrosheet events backfill
│   └── utils/
│       ├── historical_team_utils.py  # Scraping + data processing
│       ├── snowflake_utils.py         # Snowflake connection helpers
│       ├── transform_utils.py         # Cleaning + validation logic
│       ├── bref.py                     # Baseball Reference session
│       └── retrosheet.py               # Retrosheet data fetching + rolling stats
├── include/
│   └── sql/
│       ├── setup_snowflake_team_game_logs.sql      # Team game logs table setup
│       └── setup_snowflake_historical_retrosheet_events.sql  # Retrosheet events table
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
cd mlb-pipeline
cp .env.example .env
# Edit .env with your Snowflake credentials
# Add a GitHub token (GH_TOKEN) for Retrosheet API access - see .env.example
```

### 2. Set up Snowflake

Run the SQL setup scripts in your Snowflake worksheet:

- `include/sql/setup_snowflake_team_game_logs.sql` - Creates team batting/pitching logs tables
- `include/sql/setup_snowflake_historical_retrosheet_events.sql` - Creates Retrosheet events table

These create:
- The `BASEBALL` database with `HISTORICAL` schema
- `BASEBALL.HISTORICAL.TEAM_BATTING_LOGS` - Team batting game logs (Baseball Reference)
- `BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS` - Team pitching game logs (Baseball Reference)
- `BASEBALL.HISTORICAL.RETROSHEET_EVENTS` - Game events with rolling window stats (Retrosheet)

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
pip install pybaseball pandas pytest beautifulsoup4 curl_cffi pygithub
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

**Retrosheet events:**
Trigger `historical_retrosheet_events_backfill` from the Airflow UI with config:
```json
{
  "seasons": [2020, 2021, 2022, 2023]
}
```

The Retrosheet DAG fetches game event data from the [Chadwick Bureau's Retrosheet repository](https://github.com/chadwickbureau/retrosheet) via pybaseball. It includes:
- Full game box scores for both teams
- Starting lineups with player IDs, names, and defensive positions
- Manager information
- Game metadata (attendance, duration, park ID)
- Pre-computed rolling window statistics (162/90/30 game rolling sums for batting average, OBP, SLG, OPS, stolen bases, caught stealing, and errors)

## Key design decisions

**Dynamic task mapping**  
Uses Airflow 2.3+ dynamic task mapping to parallelize by team and season. Each team/season combination spawns its own task.

**Separation of concerns**  
Transforms live in `transform_utils.py`, not in the DAG. This makes them independently testable.

**TaskFlow API**  
Uses Airflow's modern `@task` decorator style rather than the older `PythonOperator`.

**Retrosheet Data Attribution**  
Retrosheet data is used under their [free use policy](https://www.retrosheet.org/#notice). The data is provided free of charge and copyrighted by Retrosheet. See `dags/utils/retrosheet.py` for the full notice.

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

-- Retrosheet events: Games with high-scoring home teams (2023 season)
SELECT DATE_DBLHEAD, TEAM_H, TEAM_V, RUNS_H, RUNS_V, DURATION
FROM BASEBALL.HISTORICAL.RETROSHEET_EVENTS
WHERE SEASON = 2023 AND RUNS_H >= 10
ORDER BY RUNS_H DESC;

-- Retrosheet rolling stats: Impact of team OBP on game outcome
SELECT
    CASE WHEN OBP_30_H >= 0.340 THEN 'Hot' ELSE 'Cold' END as home_team_obp_status,
    COUNT(*) as games,
    SUM(HOME_VICTORY) as home_wins,
    ROUND(AVG(HOME_VICTORY) * 100, 1) as home_win_pct
FROM BASEBALL.HISTORICAL.RETROSHEET_EVENTS
WHERE SEASON = 2023
GROUP BY 1;
```

## Extending this project

- Add dbt for data modeling
- Add Airflow alerting (email/Slack on failure)
- Add a Streamlit dashboard for visualization
