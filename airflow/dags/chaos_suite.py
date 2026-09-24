"""The chaos suite as a DAG: seven arms, each injected, run, measured and judged.

This is the same suite `python -m payment_ledger.chaos_run` runs, expressed as tasks. It is not a
second implementation of it. The steps come from `chaos_run.wave_steps`, `dbt_step` and
`report_step`, the same functions the command line uses, and what differs is only how a step
becomes a running container: a `docker compose run` at a terminal, a container the scheduler starts
here. A DAG that restated the pipeline in its own words would be a second definition of it, and the
two would agree right up until somebody changed one.

**Why the tasks start containers instead of doing the work.** Spark and dbt are already images, and
they are the images the ordinary run uses. An orchestrator that reimplemented them in the worker
would be scheduling a pipeline nobody else runs, and the thing it proved green would not be the
thing that ships.

**What the DAG has that the loop does not.** A run history, a task boundary around every step, and
the failure landing on the step that failed rather than at the end of a log. Injecting is separate
from publishing, publishing from the stream jobs, the ledger from the verdict, so a suite that goes
wrong says which of those went wrong. That is the whole argument for it at this size: nothing here
needs a scheduler, and one machine can only run one Spark job at a time anyway.

The arms run one after another and the baseline runs first, because every other arm compares its
counts against the baseline's.

**Trigger it with `make dag`, not from the UI on a lakehouse the last suite left behind.** An arm
delivered on top of a previous one publishes into a topic that already holds its events, and the
late arrival arm would take four closes instead of two. Clearing that is `make chaos-reset`, which
stays outside the DAG because it talks to the docker CLI and the scheduler does not have one.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

from payment_ledger import chaos, chaos_run, config

# The docker daemon binds host paths, and it knows nothing about this container's filesystem, so
# where the repository lives on the host has to be told to us. Compose refuses to start without it.
HOST_ROOT = os.environ.get("LEDGER_HOST_ROOT", "").rstrip("/\\")
if not HOST_ROOT:
    raise RuntimeError(
        "LEDGER_HOST_ROOT is empty, so this DAG cannot tell the docker daemon where the working"
        " tree is. Start the stack with it set to the absolute path of the repository on the host,"
        " for example LEDGER_HOST_ROOT=$(pwd) docker compose --profile orchestration up -d airflow."
    )
NETWORK = os.environ.get("LEDGER_NETWORK", "payment-ledger_default")
CHECKPOINTS = "payment-ledger_spark-checkpoints"

SEED = 1


def bind(path: str, target: str, *, read_only: bool = True) -> Mount:
    return Mount(source=f"{HOST_ROOT}/{path}", target=target, type="bind", read_only=read_only)


# What `docker compose run <service>` would have set up, said again here because a sibling container
# started through the API gets none of it. This is the real cost of orchestrating containers from
# inside one, and it is worth paying only because the alternative is a scheduler that runs a
# different pipeline from the one at the terminal.
SERVICES = {
    "spark": {
        "image": "payment-ledger/spark:3.5.7-iceberg1.10.1",
        "entrypoint": "/opt/spark/bin/spark-submit",
        "working_dir": "/opt/payment-ledger",
        "environment": {
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1",
            "SPARK_CONF_DIR": "/opt/payment-ledger/conf",
        },
        "mounts": [
            bind("jobs", "/opt/payment-ledger/jobs"),
            bind("conf", "/opt/payment-ledger/conf"),
            bind("data", "/opt/payment-ledger/data"),
            Mount(source=CHECKPOINTS, target="/opt/payment-ledger/checkpoints", type="volume"),
        ],
    },
    "dbt": {
        "image": "payment-ledger/dbt:1.11.0",
        # The image's own entrypoint renders the profile from the environment and execs dbt.
        "entrypoint": None,
        "working_dir": "/opt/payment-ledger/dbt",
        "environment": {"DBT_PROFILES_DIR": "/tmp/dbt", "DBT_HOST": "spark-thrift"},
        "mounts": [bind("dbt", "/opt/payment-ledger/dbt", read_only=False)],
    },
}


class TolerantDockerOperator(DockerOperator):
    """A container whose non-zero exit is an outcome rather than a failure.

    Four of the six scenarios exist to make an invariant fail, so `dbt build` coming back non-zero
    is what a working chaos suite looks like. The verdict is read from the build's own artifact,
    and a task that failed the run here would stop the suite from ever reaching it.

    It also swallows a build that never started, which the operator cannot tell apart from one that
    failed tests. That case is caught one step later instead: the record was removed before the
    build, so the verdict finds none and stops.
    """

    def execute(self, context):
        try:
            return super().execute(context)
        except AirflowException as exc:
            self.log.warning("%s exited non-zero, which this step allows: %s", self.task_id, exc)
            return None


def docker_task(step: chaos_run.Step, task_id: str) -> DockerOperator:
    service = SERVICES[step.service]
    operator = TolerantDockerOperator if step.tolerate else DockerOperator
    return operator(
        task_id=task_id,
        image=service["image"],
        entrypoint=service["entrypoint"],
        command=step.command,
        working_dir=service["working_dir"],
        environment={**service["environment"], **step.env},
        mounts=service["mounts"],
        mount_tmp_dir=False,
        network_mode=NETWORK,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        # The measurements are printed by the job and read by the next task. Every line, because
        # Spark writes plenty of its own and the one that matters announces itself.
        do_xcom_push=step.capture,
        xcom_all=step.capture,
    )


def inject(scenario: str, **_) -> None:
    manifest = chaos.build(
        scenario, config.REPO_ROOT / "data" / "generated", chaos_dir(scenario), SEED
    )
    print(f"{scenario}: {json.dumps(manifest['injected'], sort_keys=True)}")


def topic(scenario: str, **_) -> None:
    chaos_run.create_topic(scenario)


def publish(scenario: str, wave: str, **_) -> None:
    # Every wave ends in a build, and this is the one task of the wave that runs in Python, ahead
    # of that build, so it is where the previous build's record goes. A build that then fails to
    # start leaves no record behind, and the verdict stops on that instead of judging an old one.
    chaos_run.forget_run_results()
    chaos_run.publish(scenario, chaos_dir(scenario) / wave / "events.jsonl")


def judge(scenario: str, **_) -> None:
    """The verdict, from the build's artifact and the counts the report job printed.

    This is the task that fails when a scenario did not do what it said it would, and it is
    deliberately the only one that can. Everything before it is plumbing.
    """
    out = chaos_dir(scenario)
    chaos_run.keep_run_results(out, scenario)

    from airflow.operators.python import get_current_context

    logs = get_current_context()["ti"].xcom_pull(task_ids=f"{scenario}.report")
    chaos_run.keep_report("\n".join(logs or []), out)

    if chaos_run.verdict(scenario, out) != 0:
        raise AirflowException(f"{scenario} is not the run the scenario described")


def chaos_dir(scenario: str) -> Path:
    return config.REPO_ROOT / "data" / "chaos" / scenario


with DAG(
    dag_id="chaos_suite",
    description="Inject the six failures, and check each was caught and only it",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["payment-ledger", "phase-5"],
    default_args={"retries": 0, "owner": "payment-ledger"},
    doc_md=__doc__,
) as dag:
    previous = None
    for scenario in chaos.SCENARIOS.values():
        with TaskGroup(group_id=scenario.name, tooltip=scenario.failure) as arm:
            injected = PythonOperator(
                task_id="inject",
                python_callable=inject,
                op_kwargs={"scenario": scenario.name},
            )
            # Auto-creation is off on the broker, so each arm makes its own topic. Through the
            # admin API rather than by running kafka-topics.sh inside the broker, because the
            # scheduler cannot exec into a running service.
            created = PythonOperator(
                task_id="topic",
                python_callable=topic,
                op_kwargs={"scenario": scenario.name},
            )
            injected >> created
            tail = created
            for number in range(1, scenario.waves + 1):
                wave = f"wave{number}"
                published = PythonOperator(
                    task_id=f"{wave}_publish",
                    python_callable=publish,
                    op_kwargs={"scenario": scenario.name, "wave": wave},
                )
                tail >> published
                tail = published
                for step in chaos_run.wave_steps(scenario.name, wave):
                    # `wave1: silver events` is a label for a human. A task id is neither.
                    name = step.label.split(": ")[-1].replace(" ", "_")
                    job = docker_task(step, f"{wave}_{name}")
                    tail >> job
                    tail = job
                # The ledger is built after every wave, because a late arrival is only late if the
                # day was closed before it turned up.
                gold = docker_task(chaos_run.dbt_step(scenario.name), f"{wave}_gold")
                tail >> gold
                tail = gold

            report = docker_task(chaos_run.report_step(scenario.name), "report")
            verdict = PythonOperator(
                task_id="verdict",
                python_callable=judge,
                op_kwargs={"scenario": scenario.name},
            )
            tail >> report >> verdict

        if previous is not None:
            previous >> arm
        previous = arm
