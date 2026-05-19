import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.statcast_pipeline import compute_rolling_features

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    for year in range(2015, 2027):
        log.info(f"Processing year {year}")
        compute_rolling_features(0, 0, year=year)
    log.info("All years complete")
