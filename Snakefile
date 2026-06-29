# Snakefile
import yaml
configfile: "conf/config.yaml"

TABLE     = config["table"]
YEAR_MIN  = config["year_min"]
YEAR_MAX  = config["year_max"]
INPUT_DIR        = config["input_dir"]
INTERMEDIATE_DIR = config["intermediate_dir"]
OUTPUT_DIR       = config["output_dir"]

# read mode directly from the table's cleaning config, single source of truth
with open(f"conf/cleaning/{TABLE}.yaml") as f:
    MODE = yaml.safe_load(f)["mode"]

rule all:
    input:
        f"{OUTPUT_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_clean.parquet"


if MODE == "array":
    rule resolve:
        input:
            f"{INPUT_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_array.parquet"
        output:
            f"{INTERMEDIATE_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_resolved.parquet"
        shell:
            f"python run_resolve.py table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"

    rule apply_rules:
        input:
            f"{INTERMEDIATE_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_resolved.parquet"
        output:
            f"{INTERMEDIATE_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_rules.parquet"
        shell:
            f"python run_rules.py table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"

    rule entity_checks:
        input:
            f"{INTERMEDIATE_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_rules.parquet"
        output:
            f"{OUTPUT_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_clean.parquet"
        shell:
            f"python run_entity.py table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"


elif MODE == "flat":
    rule apply_rules:
        input:
            f"{INPUT_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}.parquet"
        output:
            f"{INTERMEDIATE_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_rules.parquet"
        shell:
            f"python run_rules.py table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"

    rule entity_checks:
        input:
            f"{INTERMEDIATE_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_rules.parquet"
        output:
            f"{OUTPUT_DIR}/{TABLE}_{YEAR_MIN}_{YEAR_MAX}_clean.parquet"
        shell:
            f"python run_entity.py table={TABLE} year_min={YEAR_MIN} year_max={YEAR_MAX}"

else:
    raise ValueError(f"Unknown mode '{MODE}'. Must be 'array' or 'flat'.")