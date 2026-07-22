from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.models.param import Param
from airflow.sdk import dag, task


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

DBT_CONTAINER = "sctiproject-plus-dbt"
DBT_PROJECT_DIR = "/workspace/scti_dbt"
DBT_TARGET = "dev"

DBT_PASSPHRASE = Variable.get("DBT_DEV_KEY_PASSPHRASE")


COMMON_TASK_ARGS = {
    "env": {
        "DBT_DEV_KEY_PASSPHRASE": DBT_PASSPHRASE,

        "DBT_DATA_INTERVAL_START": "{{ data_interval_start.isoformat() }}",
        "DBT_DATA_INTERVAL_END": "{{ data_interval_end.isoformat() }}",

        "DBT_FULL_REFRESH": "{{ 'true' if params.full_refresh else 'false' }}",
    },
    "append_env": True,
    "execution_timeout": timedelta(minutes=30),
}


def docker_command(inner_command: str) -> str:
    return f"""
set -e

docker exec \
    -e DBT_DEV_KEY_PASSPHRASE="$DBT_DEV_KEY_PASSPHRASE" \
    -e DBT_DATA_INTERVAL_START="$DBT_DATA_INTERVAL_START" \
    -e DBT_DATA_INTERVAL_END="$DBT_DATA_INTERVAL_END" \
    -e DBT_FULL_REFRESH="$DBT_FULL_REFRESH" \
    {DBT_CONTAINER} \
    bash -c '
        cd {DBT_PROJECT_DIR}

        {inner_command}
    '
"""


def create_build_task(task_id, selector, support_full_refresh=True):

    if support_full_refresh:

        command = f"""
if [ "$DBT_FULL_REFRESH" = "true" ]; then
    uv run dbt build \
        --selector {selector} \
        --target {DBT_TARGET} \
        --full-refresh
else
    uv run dbt build \
        --selector {selector} \
        --target {DBT_TARGET}
fi
"""

    else:

        command = f"""
uv run dbt build \
    --selector {selector} \
    --target {DBT_TARGET}
"""

    @task.bash(task_id=task_id, **COMMON_TASK_ARGS)
    def _task():
        return docker_command(command)

    return _task()


def create_snapshot_task():

    @task.bash(task_id="dbt_snapshot", **COMMON_TASK_ARGS)
    def _task():

        return docker_command(
            f"""
uv run dbt snapshot \
    --target {DBT_TARGET}
"""
        )

    return _task()


def create_docs_task():

    @task.bash(task_id="dbt_docs_generate", **COMMON_TASK_ARGS)
    def _task():

        return docker_command(
            f"""
uv run dbt docs generate \
    --target {DBT_TARGET}
"""
        )

    return _task()


@dag(
    dag_id="scti_dbt_build",
    start_date=datetime(2026, 7, 1),

    schedule="@daily",

    catchup=False,

    max_active_runs=1,

    default_args={
        "owner": "david",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },

    params={
        "full_refresh": Param(
            False,
            type="boolean",
            title="Full Refresh",
        ),
    },

    tags=[
        "dbt",
        "snowflake",
        "scti",
    ],
)
def scti_dbt_build():

    snapshot = create_snapshot_task()

    staging = create_build_task(
        "dbt_staging_build",
        "staging",
        False,
    )

    entities = create_build_task(
        "dbt_intermediate_entities_build",
        "intermediate_entities",
    )

    summaries = create_build_task(
        "dbt_intermediate_summaries_build",
        "intermediate_summaries",
    )

    marts = create_build_task(
        "dbt_mart_build",
        "marts",
        False,
    )

    docs = create_docs_task()

    (
        snapshot
        >> staging
        >> entities
        >> summaries
        >> marts
        >> docs
    )


scti_dbt_build()