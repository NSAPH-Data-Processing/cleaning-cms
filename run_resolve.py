import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from src.cleaner import resolve_table
from src.utils import setup_logging, load_run_context

logger = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    setup_logging(cfg.get("log_level", "INFO"))
    ctx = load_run_context(cfg)

    table, year_min, year_max = ctx["table"], ctx["year_min"], ctx["year_max"]
    input_path = Path(cfg.input_dir) / f"{table}_{year_min}_{year_max}_array.parquet"
    output_path = Path(cfg.intermediate_dir) / f"{table}_{year_min}_{year_max}_resolved.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info("run_resolve starting")
    logger.info("  table   : %s", table)
    logger.info("  years   : %s - %s", year_min, year_max)
    logger.info("  input   : %s", input_path)
    logger.info("  output  : %s", output_path)
    logger.info("  dry_run : %s", ctx["dry_run"])

    resolve_table(
        input_path=input_path,
        output_path=output_path,
        cleaning_config=ctx["cleaning_config"],
        dry_run=ctx["dry_run"],
    )

    logger.info("run_resolve finished.")


if __name__ == "__main__":
    main()