# Snakefile
import re
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from cleancms.utils import (
    build_input_filename,
    build_intermediate_filename,
    build_output_filename,
    build_stage_dirs,
)

configfile: "conf/config.yaml"

DATASET   = config["dataset"]
TABLE     = config["table"]
YEAR_MIN  = config["year_min"]
YEAR_MAX  = config["year_max"]

# Snakemake's configfile loader is plain YAML — it doesn't run Hydra's defaults
# composition, so conf/datapaths/{dataset}.yaml is opened directly here, the same way
# the cleaning config below is.
with open(f"conf/datapaths/{DATASET}.yaml") as _f:
    DATAPATHS_CONF = yaml.safe_load(_f)

_stage_dirs = build_stage_dirs(DATAPATHS_CONF["name"])
INPUT_DIR        = _stage_dirs["input_dir"]
INTERMEDIATE_DIR = _stage_dirs["intermediate_dir"]
OUTPUT_DIR       = _stage_dirs["output_dir"]

with open(f"conf/cleaning/{DATASET}/{TABLE}.yaml") as _f:
    CLEANING_CONF = yaml.safe_load(_f)

# 'mode' (array|flat) is the single source of truth, matching src.utils.load_run_context.
MODE = CLEANING_CONF.get("mode")
if MODE not in ("array", "flat"):
    raise ValueError(f"cleaning config for '{TABLE}' must set mode: array|flat (got {MODE!r})")
AGGREGATED = MODE == "array"

# Explicit shard list (see conf/cleaning/eligibility.yaml for an example). Tables
# without a 'shards' key are treated as a single non-sharded run.
SHARDS  = CLEANING_CONF.get("shards") or [None]
SHARDED = SHARDS != [None]


def output_path(shard):
    return f"{OUTPUT_DIR}/{build_output_filename(TABLE, YEAR_MIN, YEAR_MAX, shard)}"


rule all:
    input:
        [output_path(s) for s in SHARDS]


# Two-stage pipeline (resolve -> apply_rules) for this table, aggregated or flat: resolve_table()
# is a passthrough copy for flat tables, so there is no separate flat-mode shortcut. apply_rules
# is the final stage and writes _clean.parquet directly to OUTPUT_DIR.
if SHARDED:

    # build_*_filename's `shard` param normally takes a real value; passing the literal string
    # "{shard}" makes it emit a valid Snakemake wildcard template (e.g.
    # "eligibility_{shard}_array.parquet") instead of duplicating the naming convention here.
    rule resolve:
        input:
            f"{INPUT_DIR}/{build_input_filename(TABLE, YEAR_MIN, YEAR_MAX, '{shard}', AGGREGATED)}"
        output:
            f"{INTERMEDIATE_DIR}/{build_intermediate_filename(TABLE, YEAR_MIN, YEAR_MAX, 'resolved', '{shard}')}"
        wildcard_constraints:
            shard = "|".join(re.escape(str(s)) for s in SHARDS),
        shell:
            f"python run_resolve.py dataset={DATASET} table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX} shard={{wildcards.shard}}"

    rule apply_rules:
        input:
            f"{INTERMEDIATE_DIR}/{build_intermediate_filename(TABLE, YEAR_MIN, YEAR_MAX, 'resolved', '{shard}')}"
        output:
            f"{OUTPUT_DIR}/{build_output_filename(TABLE, YEAR_MIN, YEAR_MAX, '{shard}')}"
        wildcard_constraints:
            shard = "|".join(re.escape(str(s)) for s in SHARDS),
        shell:
            f"python run_rules.py dataset={DATASET} table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX} shard={{wildcards.shard}}"

else:
    _input_file    = f"{INPUT_DIR}/{build_input_filename(TABLE, YEAR_MIN, YEAR_MAX, None, AGGREGATED)}"
    _resolved_file = f"{INTERMEDIATE_DIR}/{build_intermediate_filename(TABLE, YEAR_MIN, YEAR_MAX, 'resolved', None)}"
    _output_file   = output_path(None)

    rule resolve:
        input:  _input_file
        output: _resolved_file
        shell:  f"python run_resolve.py dataset={DATASET} table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"

    rule apply_rules:
        input:  _resolved_file
        output: _output_file
        shell:  f"python run_rules.py dataset={DATASET} table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"
