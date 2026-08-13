"""Dagster assets loaded straight from the dbt manifest.

Every staging/mart model becomes a Dagster asset automatically, with
Dagster's dependency graph mirroring dbt's own ref()/source() lineage --
there's no hand-maintained DAG to keep in sync with the dbt project.
"""

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets

from orchestration.resources import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def tfnsw_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
