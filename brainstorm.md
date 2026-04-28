# cleaning-cms

A configurable post-normalization cleaning pipeline for CMS Medicaid TAF data.

## Position in the pipeline

```
raw TAF parquet
    │
    ▼  normalization-cms / select_variables
per-year column-selected parquet  (intermediate/)
    │
    ▼  normalization-cms / build_tables
{table}_{year_min}_{year_max}.parquet          ← flat: all rows, all years concatenated
{table}_{year_min}_{year_max}_array.parquet    ← grouped by primary_key, non-key columns
                                                  as arrays + n_distinct_* conflict indicators
    │
    ▼  cleaning-cms  (this package)
{table}_{year_min}_{year_max}_clean.parquet    ← one row per primary_key, conflicts resolved
```

The `beneficiaries` table is the primary target. `bene_id` is the primary key, and a beneficiary
appearing in five yearly DE files generates one row in the array table with five-element arrays
per non-key column. Cleaning collapses those arrays into a single canonical value per variable.

---

## Background: conflict indicators

`build_tables` emits an `n_distinct_{col}` integer alongside every aggregated column:

| `sex_cd` | `n_distinct_sex_cd` | interpretation |
|---|---|---|
| `[2, 2, 2]` | `1` | no conflict — value is stable across years |
| `[1, 2, 2]` | `2` | conflict — same beneficiary has different recorded sex |
| `[null, 2, null]` | `1` | nulls excluded from count; effectively no conflict |
| `[null, null, null]` | `0` | always missing — any strategy will return NULL |

TAF conflicts are not rare. Race/ethnicity conflicts exceed 10% in some cohorts; state code
conflicts reflect genuine moves as well as administrative re-enrollment errors. The cleaning step
must make a methodological choice for each variable — there is no universally correct answer, and
those choices should be visible in config, not buried in code.

---

## Two approaches

Both approaches operate on the `_array.parquet` produced by `build_tables` and produce a single
flat parquet with one row per `bene_id`.

**Approach A** (strategy registry): YAML maps each column to a named strategy; Python generates
the DuckDB SQL. Config is readable; logic is centralized in Python.

**Approach D** (SQL templates): YAML contains DuckDB SQL expressions directly; Python assembles
and runs them. Maximum flexibility; no abstraction layer between config and execution.

A hybrid is also described below — Approach A as the primary mode with Approach D's raw SQL
available as a per-column escape hatch. This is likely the practical choice.

---

## Approach A — Per-variable strategy registry

### What it is

A registry maps strategy names to DuckDB expression builders. The YAML config assigns one named
strategy to each column, with optional parameters (e.g., which codes count as "missing"). Python
inspects the array table's schema, looks up each column's strategy, and assembles a single
`SELECT` statement that resolves all arrays in one DuckDB pass.

### Package structure

```
cleaning-cms/
├── cleaningcms/
│   ├── __init__.py
│   ├── strategies.py       # registry: name → SQL expression builder
│   ├── clean_tables.py     # DuckDB orchestration
│   └── utils.py
├── conf/
│   ├── config.yml
│   ├── datapaths/
│   │   └── medicaid_taf_red.yaml
│   └── clean_tables/
│       └── medicaid_taf.yml
└── run_clean_tables.py     # @hydra.main entry point
```

### Config

```yaml
# conf/clean_tables/medicaid_taf.yml

title: Medicaid TAF Cleaning
tables:
  beneficiaries:
    input_pattern:  "{basepath}/beneficiaries_{year_min}_{year_max}_array.parquet"
    output_name:    "beneficiaries_{year_min}_{year_max}_clean"
    year_min: 2014
    year_max: 2018
    primary_key: [bene_id]

    # Columns not listed here receive `default_strategy` (see below).
    # An explicit listing is preferred — it forces a conscious choice per variable.
    default_strategy: null   # null means: raise an error for any unlisted column
                             # set to "mode" to silently apply mode as fallback

    variables:
      # --- Demographics ---
      bene_dob:
        strategy: min
      sex_cd:
        strategy: prefer_nonmissing
        missing_codes: [0]             # 0 = unknown; prefer 1 (male) or 2 (female)
      race_ethncty_cd:
        strategy: prefer_nonmissing
        missing_codes: [0, 6]          # 0 = unreported, 6 = other/multiracial
      state_cd:
        strategy: most_recent
      county_fips_cd:
        strategy: most_recent
      zip_cd:
        strategy: most_recent
      mdcr_bene_id:
        strategy: first_non_null

      # --- Enrollment ---
      chip_cd:
        strategy: mode
      elgblty_grp_cd:
        strategy: mode
      masboe_cd:
        strategy: mode
      mdcd_enrl_dt:
        strategy: min
      mdcd_enrl_dt_max:
        strategy: max
      dual_elgbl_cd:
        strategy: ever_nonzero
      ssdi_ind:
        strategy: ever_nonzero
      ssi_ind:
        strategy: ever_nonzero
      foster_care_ind:
        strategy: ever_nonzero

      # --- Death ---
      death_dt:
        strategy: min_if_any
      mdcd_death_dt:
        strategy: min_if_any

      # --- Conflict suppression ---
      # Set require_no_conflict: true to emit NULL instead of a resolved value
      # when n_distinct > 1. Useful when you'd rather have a missing value than
      # a potentially wrong one.
      bene_dob:
        strategy: min
        require_no_conflict: false     # default; resolve regardless
      # bene_dob:
      #   strategy: min
      #   require_no_conflict: true    # emit NULL if DOB conflicts across years

    conflict_report: true     # write sidecar CSV of bene_ids with any n_distinct > 1
    threads: 8
    memory_limit: "64GB"
```

### `cleaningcms/strategies.py`

```python
from __future__ import annotations


def strategy_mode(col: str, **_) -> str:
    return f"list_mode(list_filter({col}, x -> x IS NOT NULL))"


def strategy_min(col: str, **_) -> str:
    return f"list_min({col})"


def strategy_max(col: str, **_) -> str:
    return f"list_max({col})"


def strategy_most_recent(col: str, **_) -> str:
    # Arrays are ordered by year ascending (guaranteed by build_tables processing order).
    # Last non-null element = most recently recorded non-missing value.
    return f"list_filter({col}, x -> x IS NOT NULL)[-1]"


def strategy_first_non_null(col: str, **_) -> str:
    return f"list_filter({col}, x -> x IS NOT NULL)[1]"


def strategy_ever_nonzero(col: str, **_) -> str:
    return f"CASE WHEN list_max({col}) > 0 THEN 1 ELSE 0 END"


def strategy_min_if_any(col: str, **_) -> str:
    # Returns NULL only when every element is NULL.
    # Otherwise returns the minimum non-null value across years.
    return f"list_min(list_filter({col}, x -> x IS NOT NULL))"


def strategy_prefer_nonmissing(col: str, missing_codes: list, **_) -> str:
    # Try mode over values that are not null and not a known missing code.
    # If every year has a missing/null value, fall back to mode of whatever is present.
    codes = ", ".join(repr(c) for c in missing_codes)
    preferred = f"list_mode(list_filter({col}, x -> x IS NOT NULL AND x NOT IN ({codes})))"
    fallback  = f"list_mode(list_filter({col}, x -> x IS NOT NULL))"
    return f"COALESCE({preferred}, {fallback})"


STRATEGY_REGISTRY: dict[str, callable] = {
    "mode":              strategy_mode,
    "min":               strategy_min,
    "max":               strategy_max,
    "most_recent":       strategy_most_recent,
    "first_non_null":    strategy_first_non_null,
    "ever_nonzero":      strategy_ever_nonzero,
    "min_if_any":        strategy_min_if_any,
    "prefer_nonmissing": strategy_prefer_nonmissing,
}


def build_expression(col: str, var_conf: dict, n_distinct_col: str) -> str:
    """
    Return a SQL expression (no alias) that resolves the array column to a scalar.

    If require_no_conflict is True, wraps the expression in a CASE that emits NULL
    when n_distinct > 1, so conflicting values are surfaced as missing rather than
    silently resolved.

    Accepts a raw 'sql' key as an escape hatch for expressions the registry can't handle.
    """
    if "sql" in var_conf:
        core = var_conf["sql"]
    else:
        strategy = var_conf.get("strategy")
        if strategy not in STRATEGY_REGISTRY:
            raise ValueError(
                f"Unknown strategy '{strategy}' for column '{col}'. "
                f"Available: {sorted(STRATEGY_REGISTRY)}"
            )
        core = STRATEGY_REGISTRY[strategy](col, **var_conf)

    if var_conf.get("require_no_conflict", False):
        return f"CASE WHEN {n_distinct_col} <= 1 THEN ({core}) ELSE NULL END"
    return core
```

### `cleaningcms/clean_tables.py`

```python
from __future__ import annotations

import os
import time
import logging
from pathlib import Path

import duckdb

from cleaningcms.strategies import build_expression, STRATEGY_REGISTRY

LOGGER = logging.getLogger(__name__)


def resolve_path(pattern: str, table_conf: dict, basepath: str) -> str:
    return pattern.format(
        basepath=basepath,
        year_min=table_conf["year_min"],
        year_max=table_conf["year_max"],
    )


def get_schema(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]


def build_clean_query(con: duckdb.DuckDBPyConnection, table_conf: dict) -> str:
    pk           = list(table_conf["primary_key"])
    variables    = table_conf.get("variables", {})
    default_strat = table_conf.get("default_strategy")
    all_cols     = get_schema(con, "src")

    # Partition columns into primary key, n_distinct indicators, and data columns
    data_cols      = [c for c in all_cols if c not in pk and not c.startswith("n_distinct_")]
    indicator_cols = {c for c in all_cols if c.startswith("n_distinct_")}

    select_exprs = []
    for col in data_cols:
        n_distinct_col = f"n_distinct_{col}"
        if n_distinct_col not in indicator_cols:
            # Column was not aggregated (e.g., it is a constant across rows); select as-is.
            select_exprs.append(f"{col}")
            continue

        if col in variables:
            var_conf = variables[col]
        elif default_strat is not None:
            var_conf = {"strategy": default_strat}
        else:
            raise ValueError(
                f"Column '{col}' has no strategy configured and default_strategy is null. "
                f"Add an entry under 'variables' in the clean_tables config."
            )

        expr = build_expression(col, var_conf, n_distinct_col)
        select_exprs.append(f"({expr}) AS {col}")

    pk_expr   = ", ".join(pk)
    col_expr  = ",\n            ".join(select_exprs)
    return f"""
        CREATE OR REPLACE TABLE cleaned AS
        SELECT
            {pk_expr},
            {col_expr}
        FROM src
    """


def write_conflict_report(
    con: duckdb.DuckDBPyConnection,
    table_conf: dict,
    output_path: str,
    output_name: str,
) -> None:
    pk            = list(table_conf["primary_key"])
    all_cols      = get_schema(con, "src")
    conflict_cols = [c for c in all_cols if c.startswith("n_distinct_")]

    if not conflict_cols:
        LOGGER.warning("No n_distinct_* columns found; skipping conflict report.")
        return

    conflict_filter = " OR ".join(f"{c} > 1" for c in conflict_cols)
    select_cols     = ", ".join(pk + conflict_cols)
    report_path     = os.path.join(output_path, f"{output_name}_conflicts.csv")

    con.execute(f"""
        COPY (
            SELECT {select_cols}
            FROM src
            WHERE {conflict_filter}
        ) TO '{report_path}' (FORMAT CSV, HEADER)
    """)
    n = con.execute(f"SELECT COUNT(*) FROM src WHERE {conflict_filter}").fetchone()[0]
    LOGGER.info(f"Conflict report: {n:,} rows with at least one conflict → {report_path}")


def clean_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    table_conf: dict,
    basepath: str,
    output_path: str,
) -> None:
    start       = time.time()
    input_path  = resolve_path(table_conf["input_pattern"], table_conf, basepath)
    output_name = resolve_path(table_conf["output_name"], table_conf, basepath)

    LOGGER.info(f"\n{'='*60}")
    LOGGER.info(f"Table   : {table_name}")
    LOGGER.info(f"Input   : {input_path}")
    LOGGER.info(f"{'='*60}")

    if not Path(input_path).exists():
        raise FileNotFoundError(f"Array parquet not found: {input_path}")

    LOGGER.info("\n[1/3] Loading array parquet...")
    con.execute(f"CREATE OR REPLACE TABLE src AS SELECT * FROM read_parquet('{input_path}')")
    raw_count = con.execute("SELECT COUNT(*) FROM src").fetchone()[0]
    LOGGER.info(f"Loaded: {raw_count:,} rows (one per unique primary key)")

    if table_conf.get("conflict_report", False):
        LOGGER.info("\n[2/3] Writing conflict report...")
        write_conflict_report(con, table_conf, output_path, output_name)
    else:
        LOGGER.info("\n[2/3] Skipping conflict report (conflict_report: false)")

    LOGGER.info("\n[3/3] Resolving conflicts...")
    query = build_clean_query(con, table_conf)
    con.execute(query)

    clean_count = con.execute("SELECT COUNT(*) FROM cleaned").fetchone()[0]
    LOGGER.info(f"Output rows: {clean_count:,}")

    os.makedirs(output_path, exist_ok=True)
    out_file = os.path.join(output_path, f"{output_name}.parquet")
    con.execute(f"COPY cleaned TO '{out_file}' (FORMAT PARQUET)")
    LOGGER.info(f"Written: {out_file} | Runtime: {time.time() - start:.2f}s")
```

### `run_clean_tables.py`

```python
from __future__ import annotations

import logging
import os

import duckdb
import hydra
import yaml
from omegaconf import DictConfig, OmegaConf

from cleaningcms.clean_tables import clean_table

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Resolve array conflicts and produce a clean, flat parquet.

    Example:
      python run_clean_tables.py dataset=medicaid_taf table=beneficiaries
    """
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    table_name = str(cfg.table)

    with open(f"conf/clean_tables/{cfg.dataset}.yml") as f:
        clean_cfg = yaml.safe_load(f)

    if table_name not in clean_cfg["tables"]:
        available = list(clean_cfg["tables"])
        raise ValueError(f"Table '{table_name}' not found. Available: {available}")

    table_conf  = clean_cfg["tables"][table_name]
    basepath    = str(cfg.datapaths.dirs.output)   # normalization output is cleaning input
    output_path = str(cfg.datapaths.dirs.clean)

    os.makedirs(output_path, exist_ok=True)

    with duckdb.connect() as con:
        con.execute(f"PRAGMA threads={table_conf.get('threads', 8)}")
        con.execute(f"PRAGMA memory_limit='{table_conf.get('memory_limit', '64GB')}'")
        clean_table(
            con=con,
            table_name=table_name,
            table_conf=table_conf,
            basepath=basepath,
            output_path=output_path,
        )


if __name__ == "__main__":
    main()
```

**Usage:**
```bash
python run_clean_tables.py dataset=medicaid_taf table=beneficiaries
```

### Extending the strategy registry

To add a new strategy, add one function to `strategies.py` and one entry to `STRATEGY_REGISTRY`.
No changes to `clean_tables.py` or the runner are needed.

```python
# Example: weighted_mode — mode weighted by n_months_enrolled
# Requires n_months_enrolled to also be an array column in the array parquet.
def strategy_weighted_mode(col: str, weight_col: str, **_) -> str:
    # Zip values with weights, filter nulls, unnest, group by value, sum weights, take argmax.
    # This is complex enough that a raw SQL CTE (Approach D) may be cleaner for this case.
    return (
        f"(SELECT val FROM ("
        f"  SELECT UNNEST({col}) AS val, UNNEST({weight_col}) AS wt"
        f") WHERE val IS NOT NULL "
        f"GROUP BY val ORDER BY SUM(wt) DESC LIMIT 1)"
    )

STRATEGY_REGISTRY["weighted_mode"] = weighted_mode
```

### Caveats and failure modes

**Array ordering is implicit.** `most_recent` returns the last non-null element of the array,
which is only correct if `build_tables` processed years in ascending order. This is true today
but is not enforced or validated. A defensive check: assert that the `year` array (if it exists
in the schema) is monotonically non-decreasing for each row after loading. Alternatively,
add `year` to the array and sort before selecting:

```sql
-- Defensive most_recent using explicit year array
list_filter(
    zip(year_arr, {col}),
    x -> x[2] IS NOT NULL
)[-1][2]
```

**Silent default fallback.** Setting `default_strategy: mode` means any newly added column in the
normalization step that is not yet listed in the cleaning config will silently receive `mode`.
For date columns, `mode` is meaningless; for continuous variables, it may produce plausible but
wrong results. Setting `default_strategy: null` forces an explicit error, which is safer.

**`prefer_nonmissing` with all-missing input.** If every year for a beneficiary has a missing-code
value for `race_ethncty_cd` (e.g., all `0`s), the COALESCE fallback returns `mode` over those
missing codes, which is just `0`. This is correct behavior — there is no non-missing value to
prefer — but worth documenting in the conflict report.

**`require_no_conflict` increases null rates.** Using this flag for `bene_dob` means any
beneficiary with a DOB conflict (even a minor one, like `1985-01-01` vs `1985-01-02`)
gets a null DOB in the clean output. This may be the right choice for analyses where age
precision matters, but it should be profiled before applying broadly.

---

## Approach D — Declarative SQL templates

### What it is

The YAML config contains DuckDB SQL expressions directly. Python assembles them into a complete
query and runs it. There is no strategy registry — the config IS the logic. This eliminates
the abstraction layer entirely: what you write in YAML is exactly what executes in DuckDB.

### Two modes of Approach D

**Mode D1 — Per-column expressions** (minimal Python layer):
Each column maps to a SQL expression string. Python substitutes `{col}` and runs a single
`SELECT`. Equivalent to using the `sql` escape hatch in Approach A for every column.

**Mode D2 — Full query template** (Python only renders and runs):
The config provides the full cleaning query as a Jinja or f-string template. Python resolves
path variables and runs it. The query can use CTEs, window functions, subqueries, and
cross-column references without restriction.

### Mode D1 — Per-column expressions

**Config:**
```yaml
tables:
  beneficiaries:
    input_pattern:  "{basepath}/beneficiaries_{year_min}_{year_max}_array.parquet"
    output_name:    "beneficiaries_{year_min}_{year_max}_clean"
    year_min: 2014
    year_max: 2018
    primary_key: [bene_id]
    mode: per_column    # D1

    variables:
      bene_dob: "list_min({col})"

      sex_cd: |
        COALESCE(
          list_mode(list_filter({col}, x -> x IS NOT NULL AND x NOT IN (0))),
          list_mode(list_filter({col}, x -> x IS NOT NULL))
        )

      race_ethncty_cd: |
        COALESCE(
          list_mode(list_filter({col}, x -> x IS NOT NULL AND x NOT IN (0, 6))),
          list_mode(list_filter({col}, x -> x IS NOT NULL))
        )

      state_cd: "list_filter({col}, x -> x IS NOT NULL)[-1]"

      death_dt: "list_min(list_filter({col}, x -> x IS NOT NULL))"

      dual_elgbl_cd: "CASE WHEN list_max({col}) > 0 THEN 1 ELSE 0 END"

      mdcr_bene_id: "list_filter({col}, x -> x IS NOT NULL)[1]"

      # Cross-variable example: null out DOB if it is implausibly late relative to death date.
      # This kind of cross-column validation cannot be expressed in Approach A.
      bene_dob_validated: |
        CASE
          WHEN list_min(bene_dob) > list_min(list_filter(death_dt, x -> x IS NOT NULL))
          THEN NULL
          ELSE list_min(bene_dob)
        END
```

**Core execution:**
```python
def build_clean_query_d1(con: duckdb.DuckDBPyConnection, table_conf: dict) -> str:
    pk        = list(table_conf["primary_key"])
    variables = table_conf["variables"]
    all_cols  = [r[1] for r in con.execute("PRAGMA table_info('src')").fetchall()]
    data_cols = [c for c in all_cols if c not in pk and not c.startswith("n_distinct_")]

    exprs = []
    for col in data_cols:
        template = variables.get(col)
        if template is None:
            raise ValueError(
                f"Column '{col}' has no SQL expression in the config. "
                f"Add an entry under 'variables' or set a default."
            )
        # {col} → column name. Additional named substitutions allow cross-column refs.
        expr = template.format(col=col)
        exprs.append(f"({expr}) AS {col}")

    pk_expr  = ", ".join(pk)
    col_expr = ",\n            ".join(exprs)
    return f"""
        CREATE OR REPLACE TABLE cleaned AS
        SELECT
            {pk_expr},
            {col_expr}
        FROM src
    """
```

### Mode D2 — Full query template

**Config:**
```yaml
tables:
  beneficiaries:
    input_pattern: "{basepath}/beneficiaries_{year_min}_{year_max}_array.parquet"
    output_name:   "beneficiaries_{year_min}_{year_max}_clean"
    year_min: 2014
    year_max: 2018
    primary_key: [bene_id]
    mode: full_query    # D2

    query: |
      -- CTEs allow intermediate steps that reference each other,
      -- which is impossible in per-column mode.
      WITH
      resolved_demo AS (
        SELECT
          bene_id,
          list_min(bene_dob) AS bene_dob,
          COALESCE(
            list_mode(list_filter(sex_cd, x -> x IS NOT NULL AND x NOT IN (0))),
            list_mode(list_filter(sex_cd, x -> x IS NOT NULL))
          ) AS sex_cd,
          COALESCE(
            list_mode(list_filter(race_ethncty_cd, x -> x IS NOT NULL AND x NOT IN (0, 6))),
            list_mode(list_filter(race_ethncty_cd, x -> x IS NOT NULL))
          ) AS race_ethncty_cd,
          list_filter(state_cd, x -> x IS NOT NULL)[-1]  AS state_cd,
          list_filter(zip_cd,   x -> x IS NOT NULL)[-1]  AS zip_cd
        FROM src
      ),
      resolved_death AS (
        SELECT
          bene_id,
          list_min(list_filter(death_dt,      x -> x IS NOT NULL)) AS death_dt,
          list_min(list_filter(mdcd_death_dt, x -> x IS NOT NULL)) AS mdcd_death_dt
        FROM src
      ),
      resolved_enroll AS (
        SELECT
          bene_id,
          list_min(mdcd_enrl_dt)     AS mdcd_enrl_dt,
          list_max(mdcd_enrl_dt_max) AS mdcd_enrl_dt_max,
          CASE WHEN list_max(dual_elgbl_cd) > 0 THEN 1 ELSE 0 END AS dual_elgbl_cd,
          CASE WHEN list_max(ssdi_ind)      > 0 THEN 1 ELSE 0 END AS ssdi_ind,
          CASE WHEN list_max(ssi_ind)       > 0 THEN 1 ELSE 0 END AS ssi_ind
        FROM src
      ),
      validated AS (
        -- Cross-CTE validation: null out DOB if it is after the recorded death date.
        SELECT
          d.bene_id,
          CASE
            WHEN d.bene_dob > dt.death_dt THEN NULL
            ELSE d.bene_dob
          END AS bene_dob,
          d.sex_cd,
          d.race_ethncty_cd,
          d.state_cd,
          d.zip_cd,
          dt.death_dt,
          dt.mdcd_death_dt,
          e.mdcd_enrl_dt,
          e.mdcd_enrl_dt_max,
          e.dual_elgbl_cd,
          e.ssdi_ind,
          e.ssi_ind
        FROM resolved_demo  d
        JOIN resolved_death  dt USING (bene_id)
        JOIN resolved_enroll e  USING (bene_id)
      )
      SELECT * FROM validated
```

**Core execution (Mode D2):**
```python
def build_clean_query_d2(table_conf: dict, input_path: str) -> str:
    # The query template references 'src', which is loaded before this is called.
    # Only path variables need to be resolved here; the SQL itself is taken verbatim.
    return f"CREATE OR REPLACE TABLE cleaned AS (\n{table_conf['query']}\n)"
```

### Dry-run / SQL preview mode

Because the full query is just a string, a `--dry-run` flag can print it without executing.
This is one of the most useful debugging features of Approach D — copy the output into the
DuckDB CLI and step through it interactively.

```python
# In run_clean_tables.py, add:
if cfg.get("dry_run", False):
    query = build_clean_query_d2(table_conf, input_path)
    LOGGER.info("Dry run — query that would execute:\n%s", query)
    return
```

```bash
python run_clean_tables.py dataset=medicaid_taf table=beneficiaries dry_run=true
```

### Validation before execution

A lightweight pre-execution check: run `EXPLAIN` on the query and assert it does not error.
This catches SQL syntax errors without scanning any data.

```python
def validate_query(con: duckdb.DuckDBPyConnection, query: str) -> None:
    # Strip CREATE TABLE wrapper; EXPLAIN needs a SELECT
    select_part = query[query.lower().index("select"):]
    try:
        con.execute(f"EXPLAIN {select_part}")
    except duckdb.Error as e:
        raise ValueError(f"SQL template failed validation:\n{e}") from e
```

### Caveats and failure modes

**YAML-embedded SQL is painful to write.** Multi-line SQL in YAML requires `|` block scalars,
indentation is significant, and YAML special characters (`{`, `}`, `:`, `#`) require escaping or
quoting. A single missing quote can produce a YAML parse error with an unhelpful line number.
The experience is meaningfully worse than writing SQL in a `.sql` file.

**Errors surface late.** In Approach A, a misconfigured strategy raises a Python error before
DuckDB is ever called. In Approach D (especially D2), errors only surface when DuckDB executes
the query — after loading the full parquet. For a 15M-row table this means a multi-minute wait
before a typo in a column name is reported.

**Column coverage is not automatic.** In Approach A, `clean_tables.py` iterates over the array
table's actual schema and applies a strategy to every data column. In Approach D, only columns
explicitly listed in the SQL appear in the output. Newly added columns in normalization are
silently dropped unless the query template is updated — and there is no error to indicate this.

**Cross-CTE column references in D2 require care.** In the full-query template, `resolved_demo`,
`resolved_death`, and `resolved_enroll` can each reference `src` independently, but a column
cannot appear in two CTEs without aliasing (DuckDB will raise an ambiguity error at the JOIN step).
The discipline of partitioning columns across CTEs must be maintained manually.

---

## Comparison

| | A: Strategy registry | D1: Per-column SQL | D2: Full query template |
|---|---|---|---|
| **Input** | `_array.parquet` | `_array.parquet` | `_array.parquet` |
| **DuckDB passes** | 1 | 1 | 1 |
| **Per-variable control** | yes | yes | yes |
| **Cross-variable logic** | via `sql` escape hatch | yes (other cols in scope) | yes, across CTEs |
| **Multi-step logic (CTEs)** | no | no | yes |
| **Config readability** | high (strategy names) | medium (inline SQL) | low (full SQL in YAML) |
| **New logic needs Python change** | yes (new strategy) | no | no |
| **Schema coverage guaranteed** | yes (iterates schema) | configurable | no (manual) |
| **Pre-execution validation** | yes (Python) | partial (`EXPLAIN`) | partial (`EXPLAIN`) |
| **Error timing** | before DuckDB | at DuckDB execution | at DuckDB execution |
| **Dry-run / SQL preview** | possible but indirect | yes | yes (native) |
| **Best for** | standard demographic cleaning | per-column custom logic | multi-step or cross-column validation |

---

## Recommendation

**Start with Approach A.** It fits the existing pipeline conventions, produces explicit per-variable
documentation in the config, and catches misconfiguration before touching data. Set
`default_strategy: null` to force an explicit error for any unlisted column — this is safer than
a silent fallback.

**Use the `sql` escape hatch for the one or two variables that need it.** The `sex_cd`
`prefer_nonmissing` strategy handles the common case, but if a specific variable needs logic the
registry can't express (e.g., a weighted mode using an enrollment-months array), add a `sql:` key
to that variable's entry in the config. This gives you Approach D1 for individual columns without
switching the entire table to D.

**Graduate to D2 if and only if cross-CTE validation becomes necessary.** The most compelling
case: nulling out `bene_dob` when it is implausibly later than `death_dt`. In Approach A this
requires a post-processing pass; in D2 it is a single CASE in the `validated` CTE. If that
pattern appears for multiple variables, D2 becomes the cleaner choice — but move the SQL out of
YAML and into a `.sql` file referenced by path in the config, which avoids the YAML escaping pain.

---

## Conflict report

Write a sidecar CSV of `bene_id`s where at least one `n_distinct_* > 1`. Useful for:

- Profiling conflict rates per variable after each pipeline run
- Sensitivity analyses restricted to conflict-free beneficiaries
- Targeted validation against external sources (SSA, Medicare enrollment)

Schema (all columns come directly from the `_array.parquet`):
```
bene_id, n_distinct_sex_cd, n_distinct_race_ethncty_cd, n_distinct_state_cd, ...
```

---

## Testing

1. **Unit tests (Approach A)**: for each strategy function, pass a known array literal and assert
   the returned SQL expression produces the expected scalar when executed in DuckDB.
2. **Integration test**: construct a small synthetic `_array.parquet` with deliberate conflicts
   (e.g., `sex_cd = [1, 2, 2]`). Run the full cleaning step. Assert row count equals unique
   `bene_id` count, and that `sex_cd` resolves to `2` (mode of non-missing values).
3. **Schema coverage test (Approach A)**: assert that every data column in the array parquet
   has a configured strategy. This prevents silent drops when normalization adds a new column.
4. **Conflict rate smoke test**: after a production run, assert conflict rates for stable variables
   (`bene_dob`, `sex_cd`) are below a historical threshold. A sudden spike indicates an upstream
   change in the normalization step or raw data.
