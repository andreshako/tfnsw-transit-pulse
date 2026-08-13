"""Shared Dagster resources: the dbt project and its CLI resource.

Uses dagster_dbt.DbtProject with prepare_if_dev() so `dagster dev` always
compiles a fresh manifest locally. In CI/prod the manifest is expected to
already exist (built as part of the deploy step) rather than compiled at
Dagster startup.
"""

import os
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

dbt_project = DbtProject(project_dir=os.fspath(DBT_PROJECT_DIR))
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))
