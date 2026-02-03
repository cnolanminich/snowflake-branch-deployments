"""Snowflake Branch Pipeline Component.

This component demonstrates environment-aware Snowflake deployments using
zero-copy cloning for branch-based development workflows.

Key features:
- Automatic database selection based on deployment environment
- Support for local, branch (PR), and production deployments
- Zero-copy clone integration for cost-effective branch databases
- Demo mode for local development without Snowflake access
"""

import os
from collections.abc import Iterator
from typing import Optional

import dagster as dg
from pydantic import Field


class SnowflakeBranchPipelineComponent(dg.Component, dg.Model, dg.Resolvable):
    """A component that creates Snowflake assets with environment-aware database selection.

    This component demonstrates the branch deployment pattern where:
    - Production: Uses ANALYTICS_PROD database
    - Branch/PR: Uses a zero-copy clone database (ANALYTICS_PR_<number>)
    - Local: Uses ANALYTICS_DEV or demo mode

    Environment is detected via:
    - DAGSTER_CLOUD_DEPLOYMENT_NAME (production vs branch)
    - GITHUB_HEAD_REF or PR_NUMBER for branch database naming
    - demo_mode=true for local development without Snowflake
    """

    demo_mode: bool = Field(
        default=True,
        description="Run in demo mode with synthetic data (no Snowflake connection)",
    )
    account: str = Field(
        default="my_account",
        description="Snowflake account identifier",
    )
    user: str = Field(
        default="my_user",
        description="Snowflake username",
    )
    warehouse: str = Field(
        default="COMPUTE_WH",
        description="Snowflake warehouse name",
    )
    role: str = Field(
        default="TRANSFORMER",
        description="Snowflake role",
    )
    production_database: str = Field(
        default="ANALYTICS_PROD",
        description="Database name for production deployments",
    )
    local_database: str = Field(
        default="ANALYTICS_DEV",
        description="Database name for local development",
    )

    def _get_database_name(self) -> str:
        """Determine the appropriate database based on environment."""
        deployment_name = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "")
        pr_number = os.getenv("PR_NUMBER", "")
        github_head_ref = os.getenv("GITHUB_HEAD_REF", "")

        # Production deployment
        if deployment_name == "prod" or deployment_name == "production":
            return self.production_database

        # Branch deployment - use PR-specific database (zero-copy clone)
        if pr_number:
            return f"ANALYTICS_PR_{pr_number}"
        if github_head_ref:
            # Sanitize branch name for database naming
            safe_branch = github_head_ref.replace("/", "_").replace("-", "_").upper()[:20]
            return f"ANALYTICS_BRANCH_{safe_branch}"

        # Local development
        return self.local_database

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for Snowflake pipeline."""
        assets = list(self._create_assets())
        return dg.Definitions(assets=assets)

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
        database = self._get_database_name()

        @dg.asset(
            key=["raw", "customers"],
            group_name="ingestion",
            description="Raw customer data from source systems",
            tags={"layer": "raw", "source": "crm"},
            kinds={"snowflake"},
        )
        def raw_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Ingest raw customer data into Snowflake."""
            if demo_mode:
                context.log.info(f"[DEMO MODE] Would ingest customers to {database}.RAW.CUSTOMERS")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "RAW.CUSTOMERS",
                        "row_count": 10000,
                        "demo_mode": True,
                    }
                )

            # Production mode - would connect to Snowflake
            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return raw_customers

    def _make_raw_orders_asset(self) -> dg.AssetsDefinition:
        """Create raw orders ingestion asset."""
        demo_mode = self.demo_mode
        database = self._get_database_name()

        @dg.asset(
            key=["raw", "orders"],
            group_name="ingestion",
            description="Raw order data from e-commerce platform",
            tags={"layer": "raw", "source": "ecommerce"},
            kinds={"snowflake"},
        )
        def raw_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Ingest raw order data into Snowflake."""
            if demo_mode:
                context.log.info(f"[DEMO MODE] Would ingest orders to {database}.RAW.ORDERS")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "RAW.ORDERS",
                        "row_count": 50000,
                        "demo_mode": True,
                    }
                )

            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return raw_orders

    def _make_raw_products_asset(self) -> dg.AssetsDefinition:
        """Create raw products ingestion asset."""
        demo_mode = self.demo_mode
        database = self._get_database_name()

        @dg.asset(
            key=["raw", "products"],
            group_name="ingestion",
            description="Raw product catalog data",
            tags={"layer": "raw", "source": "catalog"},
            kinds={"snowflake"},
        )
        def raw_products(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Ingest raw product data into Snowflake."""
            if demo_mode:
                context.log.info(f"[DEMO MODE] Would ingest products to {database}.RAW.PRODUCTS")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "RAW.PRODUCTS",
                        "row_count": 500,
                        "demo_mode": True,
                    }
                )

            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return raw_products

    def _make_stg_customers_asset(self) -> dg.AssetsDefinition:
        """Create staging customers transformation asset."""
        demo_mode = self.demo_mode
        database = self._get_database_name()

        @dg.asset(
            key=["staging", "stg_customers"],
            group_name="staging",
            deps=[dg.AssetKey(["raw", "customers"])],
            description="Cleaned and standardized customer data",
            tags={"layer": "staging"},
            kinds={"snowflake", "sql"},
        )
        def stg_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Transform raw customers to staging layer."""
            if demo_mode:
                context.log.info(f"[DEMO MODE] Would transform customers in {database}")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "STAGING.STG_CUSTOMERS",
                        "row_count": 9500,
                        "demo_mode": True,
                    }
                )

            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return stg_customers

    def _make_stg_orders_asset(self) -> dg.AssetsDefinition:
        """Create staging orders transformation asset."""
        demo_mode = self.demo_mode
        database = self._get_database_name()

        @dg.asset(
            key=["staging", "stg_orders"],
            group_name="staging",
            deps=[dg.AssetKey(["raw", "orders"]), dg.AssetKey(["raw", "products"])],
            description="Enriched order data with product details",
            tags={"layer": "staging"},
            kinds={"snowflake", "sql"},
        )
        def stg_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Transform raw orders to staging layer with product enrichment."""
            if demo_mode:
                context.log.info(f"[DEMO MODE] Would transform orders in {database}")
                return dg.MaterializeResult(
                    metadata={
                        "database": database,
                        "table": "STAGING.STG_ORDERS",
                        "row_count": 48000,
                        "demo_mode": True,
                    }
                )

            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return stg_orders

    def _make_customer_lifetime_value_asset(self) -> dg.AssetsDefinition:
        """Create customer lifetime value mart asset."""
        demo_mode = self.demo_mode
        database = self._get_database_name()

        @dg.asset(
            key=["marts", "customer_lifetime_value"],
            group_name="marts",
            deps=[dg.AssetKey(["staging", "stg_customers"]), dg.AssetKey(["staging", "stg_orders"])],
            description="Customer lifetime value aggregation for analytics",
            tags={"layer": "marts", "domain": "finance"},
            kinds={"snowflake", "sql"},
        )
        def customer_lifetime_value(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Calculate customer lifetime value metrics."""
            if demo_mode:
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

            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return customer_lifetime_value

    def _make_monthly_revenue_asset(self) -> dg.AssetsDefinition:
        """Create monthly revenue mart asset."""
        demo_mode = self.demo_mode
        database = self._get_database_name()

        @dg.asset(
            key=["marts", "monthly_revenue"],
            group_name="marts",
            deps=[dg.AssetKey(["staging", "stg_orders"])],
            description="Monthly revenue aggregations for reporting",
            tags={"layer": "marts", "domain": "finance"},
            kinds={"snowflake", "sql"},
        )
        def monthly_revenue(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Aggregate monthly revenue metrics."""
            if demo_mode:
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

            raise NotImplementedError(
                "Production mode requires Snowflake connection. Set demo_mode: true"
            )

        return monthly_revenue
