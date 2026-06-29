# Cleaning-CMS

Post-normalization cleaning step for CMS-style administrative data.

Sits between `build_tables` and `materialize` in the pipeline:

```
{table}_{year_min}_{year_max}_array.parquet
    -> clean-cms
{table}_{year_min}_{year_max}_clean.parquet
```

---

## What it does

Cleaning is broken into three sequential steps:

1. **Array resolution** (`run_resolve.py`) — resolves array-valued columns (one value per primary key) using configurable strategies. Writes `_resolved.parquet`. Skipped entirely for flat tables.
2. **Across-columns checks** (`run_rules.py`) — applies logical column checks (e.g. death date after date of birth). Writes `_rules.parquet`.
3. **Entity consistency checks** (`run_entity.py`) — applies entity-level checks (e.g. enrollment period consistency). Writes `_clean.parquet`. Currently a passthrough; across-rows logic not yet implemented.

Uses **Approach A** (strategy registry) as the default design, with a `sql:` escape hatch for one-off expressions.

Each step validates the schema of its input before running, and QC/lineage columns (`n_distinct_*`) are excluded from the cleaned output.

---

## Setup

```bash
conda env create -f environment.yaml
conda activate clean-cms
```
---

## Table modes

Each table is either `array` or `flat`, declared in its cleaning config (`conf/cleaning/{table}.yaml`). This is the single source of truth for mode — the Snakefile reads it directly from there.

- **`mode: array`** — input is an `_array.parquet` from `build_tables`, where each non-key column is a list of values across years. All three steps run.
- **`mode: flat`** — input is already a flat parquet (one row per primary key). Array resolution is skipped; the pipeline routes straight to rules and entity checks.

---

## Directory setup

Data directories and symlinks are defined per table in `conf/datapaths/{table}.yaml` and created by an explicit one-time setup step:

```bash
python create_dir_paths.py datapaths={table}
```

A string path creates a symlink to external data; `null` creates a real local folder. Run this once before processing a new table or in a new environment.

---

## Running

### Via Snakemake (recommended)
```bash
# Full pipeline with defaults from conf/config.yaml
snakemake --cores 1

# Dry run: show which rules would execute without running them
snakemake --cores 1 -n

# Force a fresh run even if outputs already exist
snakemake --cores 1 --forceall

# Capture full output to a log
snakemake --cores 1 2>&1 | tee pipeline.log
```

Note: "Nothing to be done" means the outputs already exist — use `--forceall` to rerun.

### Individual steps
```bash
python run_resolve.py table=beneficiaries year_min=2008 year_max=2020
python run_rules.py   table=beneficiaries year_min=2008 year_max=2020
python run_entity.py  table=beneficiaries year_min=2008 year_max=2020

# Validate SQL without writing (resolve step only — chained steps need real intermediates)
python run_resolve.py dry_run=true
```

---

## Configuration

### Runner config: `conf/config.yaml`
Controls paths and which table/years to process. Composes the per-table datapaths and cleaning configs via Hydra defaults.

```yaml
defaults:
  - datapaths: beneficiaries
  - cleaning: beneficiaries
  - _self_

input_dir: data/normalized
intermediate_dir: data/intermediate
output_dir: data/cleaned
table: beneficiaries
year_min: 2008
year_max: 2020
dry_run: false
log_level: INFO
```

### Datapaths config: `conf/datapaths/{table}.yaml`
Defines the data directories and symlinks for a table.

```yaml
name: beneficiaries
dirs:
  normalized: /path/to/external/normalized/data   # string → symlink
  intermediate: /path/to/intermediate/data                               
  cleaned: /path/to/cleaned/data
```

### Cleaning config: `conf/cleaning/{table}.yaml`
Defines the table mode, per-column strategies, and cleaning rules.

```yaml
table: beneficiaries
mode: array
primary_key:
  - bene_id
default_strategy: mode          # fallback for columns without an explicit strategy
variables:
  sex_cd:
    strategy: prefer_nonmissing
    missing_codes: [0]
cleaning_rules:
  across_columns:
    death_after_dob:
      rule: date_order
      earlier: bene_dob
      later: death_dt
      on_violation: null_later
```

**Built-in strategies:**

| Strategy | Description |
|---|---|
| `mode` | Most frequent non-null value |
| `min` / `max` | Smallest / largest non-null value |
| `most_recent` | Last non-null value (time-ordered arrays) |
| `prefer_nonmissing` | First non-null, non-missing-code value |
| `ever_nonzero` | 1 if any value is nonzero, else 0 |
| `require_stable` | Value if stable, else NULL |

**SQL escape hatch:**
```yaml
my_col:
  sql: "list_mode(list_filter(my_col, x -> x IS NOT NULL AND x NOT IN (0, 99)))"
```

---

## Adding a new table

1. Create `conf/cleaning/{table}.yaml` with the table mode, strategies, and rules.
2. Create `conf/datapaths/{table}.yaml` with the data directories.
3. Run `python create_dir_paths.py datapaths={table}` once to set up folders/symlinks.
4. Run `snakemake --cores 1 --config table={table}`.

---

## Repo structure

```
clean-cms/
├── Snakefile                        # Snakemake orchestration (reads mode from cleaning config)
├── conf/
│   ├── config.yaml                  # Hydra runner config
│   ├── datapaths/
│   │   └── beneficiaries.yaml       # per-table data dirs + symlinks
│   └── cleaning/
│       └── beneficiaries.yaml       # per-table mode, strategies, rules
├── src/
│   └── clean_cms/
│       ├── cleaner.py               # resolve_table, apply_rules, apply_entity_checks
│       ├── executor.py              # DuckDB connection, parquet I/O, schema validation
│       ├── query_builder.py         # SQL assembly
│       ├── registry.py              # strategy registry
│       ├── resolver.py              # per-column expression resolution
│       ├── rules.py                 # cleaning rule handlers
│       └── utils.py                 # helpers, load_run_context, validate_schema
├── run_resolve.py                   # entry point — array resolution
├── run_rules.py                     # entry point — across-columns checks
├── run_entity.py                    # entry point — entity consistency checks
├── create_dir_paths.py              # one-time directory/symlink setup
├── data/
│   ├── normalized/                  # input: _array.parquet from build_tables
│   ├── intermediate/                # _resolved.parquet, _rules.parquet
│   └── cleaned/                     # output: _clean.parquet
├── environment.yaml
└── README.md
```