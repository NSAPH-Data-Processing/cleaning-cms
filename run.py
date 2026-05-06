# run.py
#
# Hydra entry point for clean-cms.
#
# Usage:
#   python run.py                                    # uses conf/config.yaml defaults
#   python run.py table=beneficiaries dry_run=true   # override on command line
#   python run.py table=mbsf year_min=2010 year_max=2019
#
# Hydra manages config composition and CLI overrides.
# Table-cleaning YAML is loaded separately with yaml.safe_load to avoid
# Hydra interpolation issues with raw SQL strings.

import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from src.clean_cms.cleaner import clean_table, load_cleaning_config
from src.clean_cms.utils import setup_logging, build_input_filename, build_output_filename

logger = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    setup_logging(cfg.get("log_level", "INFO"))

    table = cfg.table
    year_min = cfg.year_min
    year_max = cfg.year_max
    input_dir = Path(cfg.input_dir)
    output_dir = Path(cfg.output_dir)
    dry_run = cfg.get("dry_run", False)

    # Build paths
    input_filename = build_input_filename(table, year_min, year_max)
    output_filename = build_output_filename(table, year_min, year_max)
    input_path = input_dir / input_filename
    output_path = output_dir / output_filename

    logger.info("clean-cms starting")
    logger.info("  table    : %s", table)
    logger.info("  years    : %s - %s", year_min, year_max)
    logger.info("  input    : %s", input_path)
    logger.info("  output   : %s", output_path)
    logger.info("  dry_run  : %s", dry_run)

    # Load table-level cleaning config (separate from Hydra config)
    cleaning_config_path = Path("conf/cleaning") / f"{table}.yaml"
    if not cleaning_config_path.exists():
        raise FileNotFoundError(
            f"No cleaning config found for table '{table}' at {cleaning_config_path}"
        )
    cleaning_config = load_cleaning_config(cleaning_config_path)
    logger.info("Loaded cleaning config: %s", cleaning_config_path)

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
