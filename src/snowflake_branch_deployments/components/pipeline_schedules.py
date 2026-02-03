"""Pipeline schedules component for orchestrating Snowflake data pipelines."""

import dagster as dg
from pydantic import Field


class PipelineSchedulesComponent(dg.Component, dg.Model, dg.Resolvable):
    """Scheduled jobs for orchestrating the Snowflake data pipeline.

    Creates jobs and schedules that coordinate:
    1. Raw data ingestion
    2. Staging transformations
    3. Marts refresh
    4. Full pipeline execution
    """

    default_status: str = Field(
        default="STOPPED",
        description="Default status for schedules (RUNNING or STOPPED)",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for jobs and schedules."""
        status = (
            dg.DefaultScheduleStatus.RUNNING
            if self.default_status == "RUNNING"
            else dg.DefaultScheduleStatus.STOPPED
        )

        # Job definitions
        raw_ingestion_job = dg.define_asset_job(
            name="raw_ingestion_job",
            selection=dg.AssetSelection.groups("ingestion"),
            description="Ingest raw data from source systems into Snowflake",
        )

        staging_transform_job = dg.define_asset_job(
            name="staging_transform_job",
            selection=dg.AssetSelection.groups("staging"),
            description="Transform raw data into staging layer",
        )

        marts_refresh_job = dg.define_asset_job(
            name="marts_refresh_job",
            selection=dg.AssetSelection.groups("marts"),
            description="Refresh analytics marts for reporting",
        )

        full_pipeline_job = dg.define_asset_job(
            name="full_pipeline_job",
            selection=(
                dg.AssetSelection.groups("ingestion")
                | dg.AssetSelection.groups("staging")
                | dg.AssetSelection.groups("marts")
            ),
            description="Full data pipeline: ingestion through marts",
        )

        # Schedule definitions
        hourly_ingestion_schedule = dg.ScheduleDefinition(
            name="hourly_raw_ingestion",
            job=raw_ingestion_job,
            cron_schedule="0 * * * *",
            description="Hourly data ingestion from source systems",
            default_status=status,
        )

        daily_staging_schedule = dg.ScheduleDefinition(
            name="daily_staging_transforms",
            job=staging_transform_job,
            cron_schedule="0 5 * * *",
            description="Daily staging transformations at 5 AM",
            default_status=status,
        )

        daily_marts_schedule = dg.ScheduleDefinition(
            name="daily_marts_refresh",
            job=marts_refresh_job,
            cron_schedule="0 6 * * *",
            description="Daily marts refresh at 6 AM",
            default_status=status,
        )

        daily_full_pipeline_schedule = dg.ScheduleDefinition(
            name="daily_full_pipeline",
            job=full_pipeline_job,
            cron_schedule="0 4 * * *",
            description="Complete daily pipeline run at 4 AM",
            default_status=status,
        )

        return dg.Definitions(
            jobs=[
                raw_ingestion_job,
                staging_transform_job,
                marts_refresh_job,
                full_pipeline_job,
            ],
            schedules=[
                hourly_ingestion_schedule,
                daily_staging_schedule,
                daily_marts_schedule,
                daily_full_pipeline_schedule,
            ],
        )
