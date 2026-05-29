# clean-cms

Post-normalization cleaning step for CMS-style administrative data.

Sits between `build_tables` and `materialize` in the pipeline:

```
{table}_{year_min}_{year_max}_array.parquet
    -> clean-cms
{table}_{year_min}_{year_max}_clean.parquet
```

---

## What it does

1. Resolves array-valued columns (one value per primary key) using configurable strategies.
2. Applies explicit cleaning rules for data problems identified upstream (across-year inconsistencies, logical column checks).

Uses **Approach A** (strategy registry) as the default design, with a `sql:` escape hatch for one-off expressions.

---

## Setup

```bash
conda env create -f environment.yaml
conda activate clean-cms
```

---

## Running

```bash
# Default config (conf/config.yaml)
python run.py

# Override table or years
python run.py table=beneficiaries year_min=2008 year_max=2010

# Dry run: print SQL without writing output
python run.py dry_run=true
```

---

## Configuration

### Runner config: `conf/config.yaml`
Controls input/output paths, table name, and year range.

### Cleaning config: `conf/cleaning/{table}.yaml`
Defines per-column strategies and cleaning rules for each table.

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

1. Create `conf/cleaning/{table}.yaml` following the beneficiaries example.
2. Run `python run.py table={table}`.

---


## Repo structure

```
clean-cms/
├── conf/
│   ├── config.yaml              # Hydra runner config
│   └── cleaning/
│       └── beneficiaries.yaml   # per-table cleaning rules
├── src/
│   └── clean_cms/
│       ├── registry.py          # strategy registry
│       ├── rules.py             # cleaning rule handlers
│       ├── cleaner.py           # core query builder and runner
│       └── utils.py             # helpers
├── run.py                       # Hydra entry point
├── environment.yaml
└── README.md
```
