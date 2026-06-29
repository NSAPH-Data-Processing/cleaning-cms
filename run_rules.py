import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from src.cleaner import apply_rules
from src.utils import (
    setup_logging,
    load_run_context,
    build_flat_input_filename,
    build_intermediate_filename,
)

logger = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    setup_logging(cfg.get("log_level", "INFO"))
    ctx = load_run_context(cfg)

    table, year_min, year_max = ctx["table"], ctx["year_min"], ctx["year_max"]
    sharded = ctx["sharded"]
    mode = ctx["cleaning_config"].get("mode", "array")

    if mode == "array":
        input_path = Path(cfg.intermediate_dir) / build_intermediate_filename(
            table, year_min, year_max, sharded, "resolved"
        )
    else:
        input_path = Path(cfg.input_dir) / build_flat_input_filename(
            table, year_min, year_max, sharded
        )

    output_path = Path(cfg.intermediate_dir) / build_intermediate_filename(
        table, year_min, year_max, sharded, "rules"
    )

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info("run_rules starting")
    logger.info("  table   : %s", table)
    logger.info("  years   : %s - %s", year_min, year_max)
    logger.info("  sharded : %s", sharded)
    logger.info("  mode    : %s", mode)
    logger.info("  input   : %s", input_path)
    logger.info("  output  : %s", output_path)
    logger.info("  dry_run : %s", ctx["dry_run"])

    apply_rules(
        input_path=input_path,
        output_path=output_path,
        cleaning_config=ctx["cleaning_config"],
        dry_run=ctx["dry_run"],
    )

    logger.info("run_rules finished.")


if __name__ == "__main__":
    main()
