# Late-Arriving Dimensions (Inferred Members) — dbt + Snowflake

> **TL;DR** — An order can arrive *before* the customer it belongs to. Instead of dropping the
> order or pointing it at a generic "-1 Unknown" row, we create an **inferred customer row**
> (a placeholder) in `dim_customers`. When the real customer record shows up later, the same
> row is filled in with real attributes — and because the surrogate key is derived from the
> natural key, **the fact row never has to be re-pointed**.

---

## 1. The problem we are solving

In a star schema, every fact row (an order) must join to a dimension row (a customer).
In real pipelines the sources don't arrive in the same order:

```
09:05  ORD-5002 placed by CUST-103      ← order system already has it
09:20  CUST-103 created in CRM          ← customer arrives 15 min LATE
```

If the warehouse loads at 09:10, `CUST-103` does not exist yet. Common bad outcomes:

| Approach | Problem |
|---|---|
| Inner join fact → dim | `ORD-5002` silently disappears from revenue reports |
| Map to a single `-1 / Unknown` row | Order is kept, but when CUST-103 arrives you must find and **update every fact row** to the new key |
| **Inferred member (this project)** | Order is kept, linked to a placeholder row for `CUST-103` that later gets its real attributes. Fact never changes. ✅ |

This is the Kimball **"late-arriving dimension / inferred member"** pattern.

---

## 2. Project layout

```
late_dim_star_schema/
├── dbt_project.yml                   # schema + materialization config per folder
├── seeds/                            # CSVs that act as our "source systems"
│   ├── raw_customers.csv             # ← the file dbt actually loads as the customer source
│   ├── raw_customers_batch1.csv      # Batch 1: CUST-101, CUST-102 only (CUST-103 missing)
│   ├── raw_customers_batch2_late.csv # Batch 2: CUST-103 (Rohan Kulkarni) has arrived
│   ├── raw_orders.csv                # 3 orders; ORD-5002 belongs to CUST-103
│   └── raw_products.csv
├── models/
│   ├── staging/                      # views  → PUBLIC_STAGING
│   │   ├── stg_customers.sql         # trim/clean + dedupe (latest updated_at wins)
│   │   ├── stg_orders.sql
│   │   └── stg_products.sql
│   └── marts/                        # tables → PUBLIC_MARTS
│       ├── dim_customers.sql         # ★ the inferred-member logic lives here
│       ├── dim_products.sql
│       ├── fct_orders.sql
│       └── schema.yml                # tests (unique, not_null, relationships)
└── docs/README.md                    # this file
```

### Where things land in Snowflake

dbt's default `generate_schema_name` builds schemas as `<target schema>_<custom schema>`.
The target schema in `~/.dbt/profiles.yml` is `PUBLIC`, so:

| dbt config (`dbt_project.yml`) | Snowflake location | Materialization |
|---|---|---|
| `seeds: +schema: raw` | `STAR_SCHEMA_DB.PUBLIC_RAW` | table (seed) |
| `models/staging: +schema: staging` | `STAR_SCHEMA_DB.PUBLIC_STAGING` | view |
| `models/marts: +schema: marts` | `STAR_SCHEMA_DB.PUBLIC_MARTS` | table |

### How `ref()` connects everything

`{{ ref('name') }}` looks up a node by **file name without extension** (seeds and models share
one namespace). At compile time dbt replaces it with the fully-qualified table name, e.g.
`ref('raw_customers')` → `STAR_SCHEMA_DB.PUBLIC_RAW.raw_customers`, and uses the refs to build
the dependency graph (DAG), which decides run order:

```
raw_customers (seed) ─► stg_customers ─┐
raw_orders    (seed) ─► stg_orders ────┼─► dim_customers ─┐
                                       │                  ├─► fct_orders
raw_products  (seed) ─► stg_products ──┴─► dim_products ──┘
```

Note: `stg_orders` feeds **both** `dim_customers` (to discover missing customers) and `fct_orders`.

---

## 3. How the logic works

### 3.1 `dim_customers.sql` — the heart of the pattern

```
known_customers      = everything in stg_customers                  (is_inferred = false)
late_fact_customers  = customer_ids in stg_orders NOT in stg_customers
inferred_customers   = placeholder rows for those ids                (is_inferred = true)
                       full_name = 'Unknown Customer (<id>)'
                       segment   = 'Unassigned', country = 'Unknown', email = null
final                = known UNION ALL inferred
customer_key         = md5(customer_id)         ← deterministic surrogate key
```

**Why `md5(customer_id)` is the key trick:** the surrogate key depends only on the natural key,
not on load order or a sequence. So the placeholder for `CUST-103` created in Batch 1 and the
real `CUST-103` row created in Batch 2 get the **exact same `customer_key`**. Nothing in the fact
table needs fixing up.

### 3.2 `fct_orders.sql`

- Left-joins orders to `dim_customers` / `dim_products` on the natural keys and carries the
  surrogate keys (`customer_key`, `product_key`).
- `has_inferred_customer = coalesce(c.is_inferred, true)` — a flag analysts can filter on to
  see revenue attached to not-yet-complete customer data.

### 3.3 `stg_customers.sql`

Cleans the raw seed (`trim`, `lower(email)`, cast `updated_at`) and **dedupes** with
`row_number() over (partition by customer_id order by updated_at desc)`, so if a customer is
re-sent only the latest version is kept.

### 3.4 Tests (`models/marts/schema.yml`)

- `dim_customers.customer_key` / `customer_id`: `unique`, `not_null`
- `fct_orders.customer_key`: `not_null` + `relationships` → `dim_customers` — **this test proves
  no order is orphaned**, even in Batch 1 when the customer hasn't arrived yet.
- Same for `fct_orders.product_key` → `dim_products`.

---

## 4. Prerequisites (one-time setup)

1. **Python env** — managed with [uv](https://docs.astral.sh/uv/):
   ```bash
   cd late_dim_star_schema
   uv sync                     # installs dbt-snowflake from pyproject.toml / uv.lock
   ```
2. **Snowflake objects** — database `STAR_SCHEMA_DB` and warehouse `DBT_WH` must exist.
3. **dbt profile** — `~/.dbt/profiles.yml`, profile name `late_dim_star_schema`
   (must match `profile:` in `dbt_project.yml`). Uses **key-pair auth** (no password in the file):
   ```yaml
   late_dim_star_schema:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <ORG-ACCOUNT>            # e.g. ABCDEFG-XY12345, no ".snowflakecomputing.com"
         user: <USER>
         private_key_path: /Users/<you>/.ssh/snowflake/rsa_key.p8
         private_key_passphrase: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}"  # only if key is encrypted
         role: <ROLE>
         database: STAR_SCHEMA_DB
         warehouse: DBT_WH
         schema: PUBLIC
         threads: 4
   ```
   Generating the key pair (macOS):
   ```bash
   mkdir -p ~/.ssh/snowflake && cd ~/.ssh/snowflake
   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out rsa_key.p8
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   chmod 600 rsa_key.p8
   grep -v "PUBLIC KEY" rsa_key.pub | tr -d '\n' | pbcopy   # copies public key body
   ```
   Then in a Snowflake worksheet: `ALTER USER <USER> SET RSA_PUBLIC_KEY='<paste>';`
4. **Check the connection:**
   ```bash
   uv run dbt debug            # expect "All checks passed!"
   ```

---

## 5. Running the demo

### Step 1 — Batch 1: order arrives early, customer missing

`raw_customers.csv` is the file the models read. Put the Batch 1 contents in it first
(CUST-101 and CUST-102 only — **CUST-103 is missing**):

```bash
cp seeds/raw_customers_batch1.csv seeds/raw_customers.csv
uv run dbt seed --full-refresh
uv run dbt run
uv run dbt test
```

> `dbt seed` also loads `raw_customers_batch1` and `raw_customers_batch2_late` as their own
> tables in `PUBLIC_RAW`. That's harmless — no model references them; they're just the
> copies we swap into `raw_customers.csv`.

### Step 2 — Verify in Snowflake

```sql
SELECT
  f.ORDER_ID,
  f.ORDER_AMOUNT,
  f.HAS_INFERRED_CUSTOMER,
  c.CUSTOMER_ID,
  c.FULL_NAME,
  c.SEGMENT,
  c.IS_INFERRED,
  c.CUSTOMER_KEY
FROM STAR_SCHEMA_DB.PUBLIC_MARTS.FCT_ORDERS f
JOIN STAR_SCHEMA_DB.PUBLIC_MARTS.DIM_CUSTOMERS c
  ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
ORDER BY f.ORDER_ID;
```

Expected (Batch 1):

| ORDER_ID | ORDER_AMOUNT | HAS_INFERRED_CUSTOMER | CUSTOMER_ID | FULL_NAME | SEGMENT | IS_INFERRED |
|---|---|---|---|---|---|---|
| ORD-5001 | 240.00 | FALSE | CUST-101 | Aarav Sharma | Enterprise | FALSE |
| ORD-5002 | 135.00 | **TRUE** | CUST-103 | **Unknown Customer (CUST-103)** | **Unassigned** | **TRUE** |
| ORD-5003 | 120.00 | FALSE | CUST-102 | Maya Nair | SMB | FALSE |

✅ `ORD-5002` is **not lost** — it's attached to an inferred placeholder. Note its `CUSTOMER_KEY`.

### Step 3 — Batch 2: the late customer arrives

`CUST-103` (`Rohan Kulkarni`, `Mid-Market`) now exists in the source:

```bash
cp seeds/raw_customers_batch2_late.csv seeds/raw_customers.csv
uv run dbt seed --full-refresh
uv run dbt run
uv run dbt test
```

`--full-refresh` on `dbt seed` drops and recreates the seed tables so the raw table exactly
matches the CSV (without it dbt truncates and re-inserts, which breaks if the columns change).

### Step 4 — Verify again

Re-run the same query. Expected (Batch 2):

| ORDER_ID | ORDER_AMOUNT | HAS_INFERRED_CUSTOMER | CUSTOMER_ID | FULL_NAME | SEGMENT | IS_INFERRED |
|---|---|---|---|---|---|---|
| ORD-5001 | 240.00 | FALSE | CUST-101 | Aarav Sharma | Enterprise | FALSE |
| ORD-5002 | 135.00 | **FALSE** | CUST-103 | **Rohan Kulkarni** | **Mid-Market** | **FALSE** |
| ORD-5003 | 120.00 | FALSE | CUST-102 | Maya Nair | SMB | FALSE |

✅ `ORD-5002` has the **same `CUSTOMER_KEY`** as in Batch 1 (`md5('CUST-103')`), but now shows the
real name and segment, and both inferred flags flipped to `FALSE`. No fact re-keying was needed.

---

## 6. Design notes & limitations (read before reusing this pattern)

- **Full rebuild, not incremental.** `dim_customers` and `fct_orders` are `table`
  materializations rebuilt on every `dbt run`. That's why the inferred row "becomes" the real
  row: it's simply regenerated from the latest sources. For large production tables, make the
  dimension **incremental** (or use a **snapshot**) and `merge` the late record onto the
  inferred row by natural key.
- **SCD Type 1 only.** Attributes are overwritten; no history is kept. If you need to know
  *when* CUST-103 stopped being inferred, add a dbt **snapshot** on `dim_customers` (SCD2).
- **Hash keys.** `md5(customer_id)` is stable across runs and environments, which is what makes
  this work. Don't replace it with a sequence / `row_number()` — keys would shift between batches.
- **`has_inferred_customer` fallback.** `coalesce(..., true)` treats a missing dimension match as
  inferred. With the current logic every order's customer has a dim row, so it's a safety net.
- **Orders with null `customer_id`** don't get an inferred row (`where o.customer_id is not null`);
  they would fail the `not_null` test on `fct_orders.customer_key`. That's intentional, so bad
  data is visible rather than hidden.

---

## 7. Command cheat-sheet

```bash
uv run dbt debug                       # test Snowflake connection
uv run dbt seed --full-refresh         # (re)load CSVs into PUBLIC_RAW
uv run dbt run                         # build staging views + mart tables
uv run dbt test                        # run schema tests
uv run dbt build                       # seed + run + test in DAG order
uv run dbt compile -s stg_customers    # see SQL with ref() resolved (target/compiled/...)
uv run dbt docs generate && uv run dbt docs serve   # browse the lineage graph in a browser
```

## 8. Troubleshooting we actually hit

| Symptom | Cause / Fix |
|---|---|
| `NameResolutionError ... Failed to resolve '<account>.snowflakecomputing.com'` / `250001 Could not connect` | Network/DNS (Wi-Fi, VPN, proxy). Check `nslookup <account>.snowflakecomputing.com`; flush DNS with `sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder`. Use `account: ORG-ACCOUNT` without the `.snowflakecomputing.com` suffix. |
| `PythonDeprecationWarning: Boto3 will no longer support Python 3.9` | Only a warning. Move the venv to Python ≥ 3.10 when convenient (update `.python-version`, then `uv sync`). |
| Tables appear in `PUBLIC_RAW` / `PUBLIC_MARTS` instead of `RAW` / `MARTS` | Expected dbt behaviour (`<target>_<custom>` schema). Override `generate_schema_name` in `macros/` if you want exact names. |
