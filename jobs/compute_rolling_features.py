import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.statcast_pipeline import compute_rolling_features

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    START_YEAR = int(os.environ.get("START_YEAR", 2015))
    END_YEAR = int(os.environ.get("END_YEAR", 2026))
    PITCHER_ONLY = os.environ.get("PITCHER_ONLY", "false").lower() == "true"

    log.info(
        f"Computing rolling features {START_YEAR}-{END_YEAR} (pitcher_only={PITCHER_ONLY})"
    )

    for year in range(START_YEAR, END_YEAR + 1):
        log.info(f"Processing year {year}")
        compute_rolling_features(0, 0, year=year, pitcher_only=PITCHER_ONLY)

    log.info("All years complete")
