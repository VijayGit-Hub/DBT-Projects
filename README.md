# late_dim_star_schema

dbt + Snowflake demo of the **late-arriving dimension (inferred member)** pattern: orders that
arrive before their customer are linked to a placeholder customer row, which is filled in when
the real customer record lands — without re-keying the fact table.

👉 **Full walkthrough, design notes and run steps: [docs/README.md](docs/README.md)**

Quick start:
```bash
uv sync
uv run dbt debug
cp seeds/raw_customers_batch1.csv seeds/raw_customers.csv      # Batch 1 (customer missing)
uv run dbt seed --full-refresh && uv run dbt run && uv run dbt test
cp seeds/raw_customers_batch2_late.csv seeds/raw_customers.csv # Batch 2 (customer arrives)
uv run dbt seed --full-refresh && uv run dbt run && uv run dbt test
```
