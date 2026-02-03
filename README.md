# Snowflake Branch Deployments Demo

This demo showcases **Dagster's branch deployment pattern with Snowflake zero-copy cloning** for safe, isolated development and testing across local, branch, and production environments.

## Key Features

- **SnowflakeResource with environment variables** - Database configured via `SNOWFLAKE_DATABASE` env var
- **Zero-copy cloning for PRs** - Each pull request gets its own isolated database clone with no additional storage cost
- **GitHub Actions integration** - Automated branch database creation and cleanup
- **Demo mode** - Run locally without Snowflake credentials for development

## How It Works

### Resource Configuration

The `SnowflakeResource` is configured via environment variables:

```python
SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),  # Set by CI/CD
    warehouse="COMPUTE_WH",
    role="TRANSFORMER",
)
```

GitHub Actions sets `SNOWFLAKE_DATABASE` based on the deployment context:

| Environment | SNOWFLAKE_DATABASE |
|-------------|-------------------|
| **Production** | `ANALYTICS_PROD` |
| **Branch/PR** | `ANALYTICS_PR_<number>` |
| **Local** | `ANALYTICS_DEV` (default) |

### Zero-Copy Cloning Benefits

Snowflake's zero-copy cloning provides:

- **Instant creation** - Clone databases in seconds, not hours
- **Zero storage overhead** - Only modified data uses additional storage
- **Complete isolation** - PR changes don't affect production
- **Easy cleanup** - Drop the clone when PR is merged/closed

## Project Structure

```
snowflake-branch-deployments/
├── .github/
│   └── workflows/
│       ├── branch-deployment.yml    # PR database creation & Dagster branch deploy
│       └── production-deployment.yml # Main branch production deploy
├── src/
│   └── snowflake_branch_deployments/
│       ├── components/
│       │   ├── snowflake_pipeline.py     # Assets using SnowflakeResource
│       │   └── pipeline_schedules.py     # Jobs and schedules
│       └── defs/
│           └── pipeline/
│               └── defs.yaml             # Component configuration
└── pyproject.toml
```

## Assets

| Asset | Layer | Description |
|-------|-------|-------------|
| `raw/customers` | Ingestion | Customer data from CRM |
| `raw/orders` | Ingestion | Order data from e-commerce |
| `raw/products` | Ingestion | Product catalog data |
| `staging/stg_customers` | Staging | Cleaned customer data |
| `staging/stg_orders` | Staging | Enriched order data |
| `marts/customer_lifetime_value` | Marts | CLV aggregations |
| `marts/monthly_revenue` | Marts | Revenue metrics |

## Getting Started

### Local Development (Demo Mode)

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Start Dagster:
   ```bash
   uv run dg dev
   ```

3. Open http://localhost:3000 - assets will run in demo mode

### Production Setup

1. Set up GitHub secrets:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ROLE`
   - `SNOWFLAKE_WAREHOUSE`
   - `DAGSTER_CLOUD_API_TOKEN`
   - `DAGSTER_CLOUD_URL`

2. Update `defs.yaml` to disable demo mode:
   ```yaml
   attributes:
     demo_mode: false
   ```

3. Push to trigger GitHub Actions

## Branch Deployment Workflow

### When a PR is opened:

1. GitHub Action creates a zero-copy clone: `ANALYTICS_PR_<number>`
2. Dagster branch deployment is created with `SNOWFLAKE_DATABASE` set to the clone
3. Comment posted to PR with database and deployment links

### When working on a PR:

- All asset materializations use the `SnowflakeResource` configured for the branch
- SQL queries automatically target the branch database
- Production data remains unchanged

### When PR is merged/closed:

1. Branch database is dropped automatically
2. Dagster branch deployment is removed
3. Cleanup comment posted to PR

## Schedules

| Schedule | Cron | Description |
|----------|------|-------------|
| `hourly_raw_ingestion` | `0 * * * *` | Hourly data ingestion |
| `daily_staging_transforms` | `0 5 * * *` | Daily staging refresh at 5 AM |
| `daily_marts_refresh` | `0 6 * * *` | Daily marts refresh at 6 AM |
| `daily_full_pipeline` | `0 4 * * *` | Full pipeline at 4 AM |

## Learn More

- [Dagster Branch Deployments](https://docs.dagster.io/guides/deploy/managing-multiple-environments)
- [Snowflake Zero-Copy Cloning](https://docs.snowflake.com/en/user-guide/object-clone)
- [Dagster + Snowflake Integration](https://docs.dagster.io/integrations/libraries/snowflake)
