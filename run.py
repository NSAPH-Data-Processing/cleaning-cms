# run.py
#
# Hydra entry point for clean-cms (legacy combined runner).
#
# Usage:
#   python run.py                                    # uses conf/config.yaml defaults
#   python run.py table=beneficiaries dry_run=true   # override on command line
#   python run.py table=mbsf year_min=2010 year_max=2019
#   python run.py sharded=true                       # one file per year in [year_min, year_max]
#
# In sharded mode each year in [year_min, year_max] is processed independently,
# producing one output file per year. In unsharded mode a single combined file
# is produced as before.

import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from src.cleaner import clean_table, load_cleaning_config
from src.utils import (
    setup_logging,
    build_input_filename,
    build_output_filename,
)

logger = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    setup_logging(cfg.get("log_level", "INFO"))

    table = cfg.table
    year_min = int(cfg.year_min)
    year_max = int(cfg.year_max)
    sharded = bool(cfg.get("sharded", False))
    input_dir = Path(cfg.input_dir)
    output_dir = Path(cfg.output_dir)
    dry_run = cfg.get("dry_run", False)

    cleaning_config_path = Path("conf/cleaning") / f"{table}.yaml"
    if not cleaning_config_path.exists():
        raise FileNotFoundError(
            f"No cleaning config found for table '{table}' at {cleaning_config_path}"
        )
    cleaning_config = load_cleaning_config(cleaning_config_path)
    logger.info("Loaded cleaning config: %s", cleaning_config_path)

    years = range(year_min, year_max + 1) if sharded else [(year_min, year_max)]

    for year_entry in years:
        if sharded:
            yr_min = yr_max = year_entry
        else:
            yr_min, yr_max = year_entry

        input_path = input_dir / build_input_filename(table, yr_min, yr_max, sharded)
        output_path = output_dir / build_output_filename(table, yr_min, yr_max, sharded)

        logger.info("clean-cms starting")
        logger.info("  table    : %s", table)
        logger.info("  years    : %s - %s", yr_min, yr_max)
        logger.info("  sharded  : %s", sharded)
        logger.info("  input    : %s", input_path)
        logger.info("  output   : %s", output_path)
        logger.info("  dry_run  : %s", dry_run)

        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        clean_table(
            input_path=input_path,
            output_path=output_path,
            cleaning_config=cleaning_config,
            dry_run=dry_run,
        )

    logger.info("clean-cms finished.")


if __name__ == "__main__":
    main()
