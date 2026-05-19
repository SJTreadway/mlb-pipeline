import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.statcast_pipeline import compute_rolling_features

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info("Starting rolling features computation")
    compute_rolling_features(0, 0)
    log.info("Rolling features computation complete")
