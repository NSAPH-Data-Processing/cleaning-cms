import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from src.cleaner import apply_entity_checks
from src.utils import setup_logging, load_run_context, build_intermediate_filename, build_output_filename

logger = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    setup_logging(cfg.get("log_level", "INFO"))
    ctx = load_run_context(cfg)

    table, year_min, year_max = ctx["table"], ctx["year_min"], ctx["year_max"]
    sharded = ctx["sharded"]
    input_path = Path(cfg.intermediate_dir) / build_intermediate_filename(
        table, year_min, year_max, sharded, "rules"
    )
    output_path = Path(cfg.output_dir) / build_output_filename(table, year_min, year_max, sharded)

    if not input_path.exists():
        raise FileNotFoundError(f"Rules file not found: {input_path}")

    logger.info("run_entity starting")
    logger.info("  table   : %s", table)
    logger.info("  years   : %s - %s", year_min, year_max)
    logger.info("  sharded : %s", sharded)
    logger.info("  input   : %s", input_path)
    logger.info("  output  : %s", output_path)
    logger.info("  dry_run : %s", ctx["dry_run"])

    apply_entity_checks(
        input_path=input_path,
        output_path=output_path,
        cleaning_config=ctx["cleaning_config"],
        dry_run=ctx["dry_run"],
    )

    logger.info("run_entity finished.")


if __name__ == "__main__":
    main()
