import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from cleancms.cleaner import apply_rules
from cleancms.utils import (
    setup_logging,
    load_run_context,
    build_intermediate_filename,
    build_output_filename,
)

logger = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    setup_logging(cfg.get("log_level", "INFO"))
    ctx = load_run_context(cfg)

    table, year_min, year_max = ctx["table"], ctx["year_min"], ctx["year_max"]
    shard = ctx["shard"]

    input_path  = Path(ctx["intermediate_dir"]) / build_intermediate_filename(table, year_min, year_max, "resolved", shard)
    output_path = Path(ctx["output_dir"]) / build_output_filename(table, year_min, year_max, shard)

    if not input_path.exists():
        raise FileNotFoundError(f"Resolved file not found: {input_path}")

    logger.info("run_rules starting")
    logger.info("  table   : %s", table)
    logger.info("  years   : %s - %s", year_min, year_max)
    logger.info("  shard   : %s", shard)
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
