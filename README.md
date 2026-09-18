# Cleaning-CMS

Post-normalization cleaning step for CMS-style administrative data.

Sits between `build_tables` and `materialize` in the pipeline. Input files may be a single
file covering the full year range, or — since normalization-cms now supports sharding
output by year (or another primary-key column) — one file per shard:

```
{table}_{year_min}_{year_max}_array.parquet        (non-sharded)
  or
{table}_{shard}_array.parquet                       (sharded, one per shard value)
    -> clean-cms
{table}_{year_min}_{year_max}_clean.parquet         (non-sharded)
  or
{table}_{shard}_clean.parquet                       (sharded, one per shard value)
```

Cleaning-cms processes each input file independently, sharding is a passthrough concern
here, not something cleaning-cms itself decides. It reads whichever shard values a table's
config declares, and produces one cleaned output per shard.

---

## What it does

Cleaning is a **two-stage** pipeline:

1. **Array resolution** (`run_resolve.py` → `resolve_table`) — resolves array-valued
   columns (one value per primary key) using configurable strategies, and applies any
   `across_years` rules. Writes `_resolved.parquet`. For flat tables there are no array
   columns, so this stage is a clean passthrough copy.
2. **Across-columns checks + final output** (`run_rules.py` → `apply_rules`) — applies
   `across_columns` rules (e.g. death date after date of birth) to the resolved
   intermediate and writes the final `_clean.parquet`. If no `across_columns` rules are
   configured, data passes through unchanged.

Entity-level consistency checks (e.g. enrollment period consistency across rows) will
mostly be part of QC rather than a stage in this pipeline, they're meant to surface
insights on materialized views, not to gate or alter cleaning-cms's output.

Uses **Approach A** (strategy registry) as the default design, with a `sql:` escape hatch
for one-off expressions.

Each step validates the schema of its input before running, and QC/lineage columns
(`n_distinct_*`) are excluded from the cleaned output.

---

## Setup

```bash
micromamba activate nsaph_data_cms_data_prep
```
---

## Table modes

Each table is either `array` or `flat`, declared in its cleaning config
(`conf/cleaning/{table}.yaml`). This is the single source of truth for mode, both the
Snakefile and `src/utils.py`'s `load_run_context` read it directly from there.

- **`mode: array`** input is an `_array.parquet` from `build_tables`, where each
  non-key column is a list of values across years. Array resolution runs.
- **`mode: flat`** input is already a flat parquet (one row per primary key). Array
  resolution is a passthrough copy; the pipeline routes straight to across-columns rules.

---

## Sharded tables

A table's cleaning config can optionally declare a `shards:` list, the set of shard
values that table's normalization-cms output was split into (typically years, matching
`shard_by` on the normalization side). Tables without a `shards` key are treated as a
single non-sharded run.

```yaml
# conf/cleaning/enrollments.yaml
shards: [2000, 2001]
```

When `shards` is set, the Snakefile generates `resolve` and `apply_rules` jobs per shard
(wildcarded on `{shard}`), and file naming switches from the `{year_min}_{year_max}`
convention to a per-shard convention:

| | non-sharded | sharded |
|---|---|---|
| input | `{table}_{year_min}_{year_max}[_array].parquet` | `{table}_{shard}[_array].parquet` |
| intermediate | `{table}_{year_min}_{year_max}_resolved.parquet` | `{table}_{shard}_resolved.parquet` |
| output | `{table}_{year_min}_{year_max}_clean.parquet` | `{table}_{shard}_clean.parquet` |

**The `shards:` list must be kept in sync by hand** with the corresponding table's
`year_min`/`year_max`/`shard_by` in normalization-cms's `conf/build_tables/{dataset}.yaml`
— there's no automatic cross-repo check. If normalization-cms's config is later extended
to a wider year range, `shards:` here needs a matching update or cleaning-cms will simply
never look for the new years' files.

**Known gap:** cleaning configs are keyed only by table name (`conf/cleaning/{table}.yaml`),
with no dataset namespace. This means a table name like `enrollments` can only describe
one dataset's schema/shards at a time, if two datasets (e.g. `medicaid_max-taf` and
`medicare_mbsf`) both have an `enrollments` table with different columns or primary keys,
switching between them means overwriting the same config file. There's currently no
config-level way to distinguish them.

---

## Directory setup

Data directories and symlinks are defined per table in `conf/datapaths/{table}.yaml` and
created by an explicit one-time setup step:

```bash
python create_dir_paths.py datapaths={table}
```

A string path creates a symlink to external data; `null` creates a real local folder. Run
this once before processing a new table or in a new environment.

---

## Running

### Via Snakemake (recommended)
```bash
# Full pipeline with defaults from conf/config.yaml — for a sharded table, this
# produces one cleaned output per shard; for a non-sharded table, a single output.
snakemake --cores 1

# Dry run: show which rules would execute without running them
snakemake --cores 1 -n

# Force a fresh run even if outputs already exist
snakemake --cores 1 --forceall

# Capture full output to a log
snakemake --cores 1 2>&1 | tee pipeline.log

# Run a different table than the one in conf/config.yaml
snakemake --cores 1 --config table=enrollments year_min=2000 year_max=2001

# Target one shard's output directly (sharded tables only)
snakemake --cores 1 data/mbsf/cleaned/enrollments_2000_clean.parquet
```

Note: "Nothing to be done" means the outputs already exist, use `--forceall` to rerun.

### Via SLURM
```bash
sbatch snakemake.sbatch
```

### Individual steps
```bash
python run_resolve.py table=beneficiaries year_min=2008 year_max=2020
python run_rules.py   table=beneficiaries year_min=2008 year_max=2020

# For a sharded table, pass shard explicitly (overrides conf/config.yaml's shard: null)
python run_resolve.py table=enrollments year_min=2000 year_max=2001 shard=2000
python run_rules.py   table=enrollments year_min=2000 year_max=2001 shard=2000

# Validate SQL without writing (resolve step only — chained steps need real intermediates)
python run_resolve.py dry_run=true
```

---

## Configuration

### Runner config: `conf/config.yaml`
Controls paths and which table/years/shard to process. Composes the per-table datapaths
and cleaning configs via Hydra defaults.

```yaml
defaults:
  - cleaning: enrollments
  - datapaths: mbsf
  - _self_

input_dir: data/mbsf/input
intermediate_dir: data/mbsf/intermediate
output_dir: data/mbsf/cleaned

table: enrollments
year_min: 2000
year_max: 2001
shard: null          # set to a shard value (e.g. 2000) to process a single shard directly;
                      # left null, Snakemake enumerates all shards from the table's cleaning config

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
Defines the table mode, shard list (if applicable), per-column strategies, and cleaning
rules.

```yaml
table: enrollments
mode: array

# Omit entirely for non-sharded tables.
shards: [2000, 2001]

primary_key:
  - bene_id
  - year

default_strategy: null   # or e.g. "mode" as a fallback for columns without an explicit strategy

variables:
  sex_cd:
    strategy: prefer_nonmissing
    missing_codes: [0]

cleaning_rules:
  across_years: {}
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

1. Create `conf/cleaning/{table}.yaml` with the table mode, `shards:` (if the table is
   sharded upstream), strategies, and rules.
2. Create `conf/datapaths/{table}.yaml` with the data directories.
3. Run `python create_dir_paths.py datapaths={table}` once to set up folders/symlinks.
4. Run `snakemake --cores 1 --config table={table}`.

---

## Repo structure

```
clean-cms/
├── Snakefile                        # Snakemake orchestration; branches into sharded vs.
│                                     # non-sharded rule sets based on each table's cleaning
│                                     # config (reads mode + shards from conf/cleaning/{table}.yaml)
├── snakemake.sbatch                  # SLURM submission wrapper
├── conf/
│   ├── config.yaml                  # Hydra runner config (table, year_min/max, shard, paths)
│   ├── datapaths/
│   │   └── mbsf.yaml                # per-table/per-dataset data dirs + symlinks
│   └── cleaning/
│       ├── beneficiaries.yaml       # flat, non-sharded
│       └── enrollments.yaml         # array, sharded by year
├── src/
│   └── clean_cms/
│       ├── cleaner.py               # resolve_table, apply_rules
│       ├── executor.py              # DuckDB connection, parquet I/O, schema validation
│       ├── query_builder.py         # SQL assembly
│       ├── registry.py              # strategy registry
│       ├── resolver.py              # per-column expression resolution
│       ├── rules.py                 # cleaning rule handlers
│       └── utils.py                 # helpers, load_run_context, validate_schema,
│                                     # build_input/intermediate/output_filename (shard-aware)
├── run_resolve.py                   # entry point — array resolution
├── run_rules.py                     # entry point — across-columns checks + final output
├── create_dir_paths.py              # one-time directory/symlink setup
├── data/
│   ├── normalized/ (or e.g. data/mbsf/input/)      # input: _array.parquet from build_tables
│   ├── intermediate/                                # _resolved.parquet
│   └── cleaned/                                     # output: _clean.parquet
└── README.md
```