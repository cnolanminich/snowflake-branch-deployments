"""Snowflake Branch Pipeline Component.

This component demonstrates environment-aware Snowflake deployments using
zero-copy cloning for branch-based development workflows.

Key features:
- SnowflakeResource configured via environment variables
- Database selection controlled by SNOWFLAKE_DATABASE env var
- GitHub Actions sets the database based on deployment context
- Demo mode for local development without Snowflake access
"""

import os
from collections.abc import Iterator

import dagster as dg
from dagster_snowflake import SnowflakeResource
from pydantic import Field


class SnowflakeBranchPipelineComponent(dg.Component, dg.Model, dg.Resolvable):
    """A component that creates Snowflake assets with environment-aware database selection.

    This component uses the SnowflakeResource configured via environment variables.
    The database is controlled by SNOWFLAKE_DATABASE, which is set by GitHub Actions:

    - Production: SNOWFLAKE_DATABASE=ANALYTICS_PROD
    - Branch/PR: SNOWFLAKE_DATABASE=ANALYTICS_PR_<number>
    - Local: SNOWFLAKE_DATABASE=ANALYTICS_DEV (or use demo_mode=true)

    When demo_mode=true, no Snowflake connection is required.
    """

    demo_mode: bool = Field(
        default=True,
        description="Run in demo mode with synthetic data (no Snowflake connection)",
    )
    warehouse: str = Field(
        default="COMPUTE_WH",
        description="Snowflake warehouse name",
    )
    role: str = Field(
        default="TRANSFORMER",
        description="Snowflake role",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for Snowflake pipeline."""
        assets = list(self._create_assets())

        # Configure SnowflakeResource via environment variables
        # SNOWFLAKE_DATABASE is set by GitHub Actions based on environment
        # In demo mode, we still provide the resource but won't actually use it
        snowflake_resource = SnowflakeResource(
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT").get_value(default="demo_account"),
            user=dg.EnvVar("SNOWFLAKE_USER").get_value(default="demo_user"),
            password=dg.EnvVar("SNOWFLAKE_PASSWORD").get_value(default="demo_password"),
            database=dg.EnvVar("SNOWFLAKE_DATABASE").get_value(default="ANALYTICS_DEV"),
            warehouse=self.warehouse,
            role=self.role,
        )

        return dg.Definitions(
            assets=assets,
            resources={"snowflake": snowflake_resource},
        )

    def _create_assets(self) -> Iterator[dg.AssetsDefinition]:
        """Generate all Snowflake pipeline assets."""
        # Raw layer
        yield self._make_raw_customers_asset()
        yield self._make_raw_orders_asset()
        yield self._make_raw_products_asset()

        # Staging layer
        yield self._make_stg_customers_asset()
        yield self._make_stg_orders_asset()

        # Marts layer
        yield self._make_customer_lifetime_value_asset()
        yield self._make_monthly_revenue_asset()

    def _make_raw_customers_asset(self) -> dg.AssetsDefinition:
        """Create raw customers ingestion asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["raw", "customers"],
            group_name="ingestion",
            description="Raw customer data from source systems",
            tags={"layer": "raw", "source": "crm"},
            kinds={"snowflake"},
        )
        def raw_customers(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Ingest raw customer data into Snowflake."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would ingest customers to {database}.RAW.CUSTOMERS")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "RAW.CUSTOMERS",
                        "row_count": 10000,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
                        customer_id VARCHAR,
                        name VARCHAR,
                        email VARCHAR,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP
                    )
                """)
                result = cursor.execute("SELECT COUNT(*) FROM RAW.CUSTOMERS")
                row_count = result.fetchone()[0]

                # Get current database for metadata
                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "RAW.CUSTOMERS",
                    "row_count": row_count,
                }
            )

        return raw_customers

    def _make_raw_orders_asset(self) -> dg.AssetsDefinition:
        """Create raw orders ingestion asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["raw", "orders"],
            group_name="ingestion",
            description="Raw order data from e-commerce platform",
            tags={"layer": "raw", "source": "ecommerce"},
            kinds={"snowflake"},
        )
        def raw_orders(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Ingest raw order data into Snowflake."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would ingest orders to {database}.RAW.ORDERS")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "RAW.ORDERS",
                        "row_count": 50000,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS RAW.ORDERS (
                        order_id VARCHAR,
                        customer_id VARCHAR,
                        order_date TIMESTAMP,
                        total_amount DECIMAL(10,2),
                        status VARCHAR
                    )
                """)
                result = cursor.execute("SELECT COUNT(*) FROM RAW.ORDERS")
                row_count = result.fetchone()[0]

                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "RAW.ORDERS",
                    "row_count": row_count,
                }
            )

        return raw_orders

    def _make_raw_products_asset(self) -> dg.AssetsDefinition:
        """Create raw products ingestion asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["raw", "products"],
            group_name="ingestion",
            description="Raw product catalog data",
            tags={"layer": "raw", "source": "catalog"},
            kinds={"snowflake"},
        )
        def raw_products(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Ingest raw product data into Snowflake."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would ingest products to {database}.RAW.PRODUCTS")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "RAW.PRODUCTS",
                        "row_count": 500,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS RAW.PRODUCTS (
                        product_id VARCHAR,
                        name VARCHAR,
                        category VARCHAR,
                        price DECIMAL(10,2)
                    )
                """)
                result = cursor.execute("SELECT COUNT(*) FROM RAW.PRODUCTS")
                row_count = result.fetchone()[0]

                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "RAW.PRODUCTS",
                    "row_count": row_count,
                }
            )

        return raw_products

    def _make_stg_customers_asset(self) -> dg.AssetsDefinition:
        """Create staging customers transformation asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["staging", "stg_customers"],
            group_name="staging",
            deps=[dg.AssetKey(["raw", "customers"])],
            description="Cleaned and standardized customer data",
            tags={"layer": "staging"},
            kinds={"snowflake", "sql"},
        )
        def stg_customers(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Transform raw customers to staging layer."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would transform customers in {database}")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "STAGING.STG_CUSTOMERS",
                        "row_count": 9500,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS AS
                    SELECT
                        customer_id,
                        TRIM(name) as customer_name,
                        LOWER(email) as email,
                        created_at,
                        updated_at
                    FROM RAW.CUSTOMERS
                    WHERE customer_id IS NOT NULL
                """)
                result = cursor.execute("SELECT COUNT(*) FROM STAGING.STG_CUSTOMERS")
                row_count = result.fetchone()[0]

                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "STAGING.STG_CUSTOMERS",
                    "row_count": row_count,
                }
            )

        return stg_customers

    def _make_stg_orders_asset(self) -> dg.AssetsDefinition:
        """Create staging orders transformation asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["staging", "stg_orders"],
            group_name="staging",
            deps=[dg.AssetKey(["raw", "orders"]), dg.AssetKey(["raw", "products"])],
            description="Enriched order data with product details",
            tags={"layer": "staging"},
            kinds={"snowflake", "sql"},
        )
        def stg_orders(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Transform raw orders to staging layer with product enrichment."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would transform orders in {database}")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "STAGING.STG_ORDERS",
                        "row_count": 48000,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE OR REPLACE TABLE STAGING.STG_ORDERS AS
                    SELECT
                        o.order_id,
                        o.customer_id,
                        o.order_date,
                        o.total_amount,
                        o.status,
                        DATE_TRUNC('month', o.order_date) as order_month
                    FROM RAW.ORDERS o
                    WHERE o.order_id IS NOT NULL
                """)
                result = cursor.execute("SELECT COUNT(*) FROM STAGING.STG_ORDERS")
                row_count = result.fetchone()[0]

                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "STAGING.STG_ORDERS",
                    "row_count": row_count,
                }
            )

        return stg_orders

    def _make_customer_lifetime_value_asset(self) -> dg.AssetsDefinition:
        """Create customer lifetime value mart asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["marts", "customer_lifetime_value"],
            group_name="marts",
            deps=[dg.AssetKey(["staging", "stg_customers"]), dg.AssetKey(["staging", "stg_orders"])],
            description="Customer lifetime value aggregation for analytics",
            tags={"layer": "marts", "domain": "finance"},
            kinds={"snowflake", "sql"},
        )
        def customer_lifetime_value(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Calculate customer lifetime value metrics."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would calculate CLV in {database}")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "MARTS.CUSTOMER_LIFETIME_VALUE",
                        "row_count": 9500,
                        "avg_clv": 1250.00,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE OR REPLACE TABLE MARTS.CUSTOMER_LIFETIME_VALUE AS
                    SELECT
                        c.customer_id,
                        c.customer_name,
                        c.email,
                        COUNT(DISTINCT o.order_id) as total_orders,
                        SUM(o.total_amount) as lifetime_value,
                        AVG(o.total_amount) as avg_order_value,
                        MIN(o.order_date) as first_order_date,
                        MAX(o.order_date) as last_order_date,
                        DATEDIFF('day', MIN(o.order_date), MAX(o.order_date)) as customer_tenure_days
                    FROM STAGING.STG_CUSTOMERS c
                    LEFT JOIN STAGING.STG_ORDERS o ON c.customer_id = o.customer_id
                    GROUP BY c.customer_id, c.customer_name, c.email
                """)
                result = cursor.execute(
                    "SELECT COUNT(*), AVG(lifetime_value) FROM MARTS.CUSTOMER_LIFETIME_VALUE"
                )
                row = result.fetchone()
                row_count = row[0]
                avg_clv = float(row[1]) if row[1] else 0

                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "MARTS.CUSTOMER_LIFETIME_VALUE",
                    "row_count": row_count,
                    "avg_clv": avg_clv,
                }
            )

        return customer_lifetime_value

    def _make_monthly_revenue_asset(self) -> dg.AssetsDefinition:
        """Create monthly revenue mart asset."""
        demo_mode = self.demo_mode

        @dg.asset(
            key=["marts", "monthly_revenue"],
            group_name="marts",
            deps=[dg.AssetKey(["staging", "stg_orders"])],
            description="Monthly revenue aggregations for reporting",
            tags={"layer": "marts", "domain": "finance"},
            kinds={"snowflake", "sql"},
        )
        def monthly_revenue(
            context: dg.AssetExecutionContext,
            snowflake: SnowflakeResource = None,
        ) -> dg.MaterializeResult:
            """Aggregate monthly revenue metrics."""
            if demo_mode:
                database = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
                context.log.info(f"[DEMO MODE] Would calculate monthly revenue in {database}")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "MARTS.MONTHLY_REVENUE",
                        "row_count": 24,
                        "total_revenue": 1500000.00,
                        "demo_mode": True,
                    }
                )

            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE OR REPLACE TABLE MARTS.MONTHLY_REVENUE AS
                    SELECT
                        order_month,
                        COUNT(DISTINCT order_id) as total_orders,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        SUM(total_amount) as total_revenue,
                        AVG(total_amount) as avg_order_value
                    FROM STAGING.STG_ORDERS
                    GROUP BY order_month
                    ORDER BY order_month
                """)
                result = cursor.execute(
                    "SELECT COUNT(*), SUM(total_revenue) FROM MARTS.MONTHLY_REVENUE"
                )
                row = result.fetchone()
                row_count = row[0]
                total_revenue = float(row[1]) if row[1] else 0

                db_result = cursor.execute("SELECT CURRENT_DATABASE()")
                database = db_result.fetchone()[0]

            return dg.MaterializeResult(
                metadata={
                    "database": database,
                    "table": "MARTS.MONTHLY_REVENUE",
                    "row_count": row_count,
                    "total_revenue": total_revenue,
                }
            )

        return monthly_revenue
