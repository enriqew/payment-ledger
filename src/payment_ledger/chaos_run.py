"""Running the chaos suite: one arm at a time, each into a lakehouse of its own.

The injection lives next door in `chaos.py` and knows nothing about Docker. This is the half that
does: it takes a scenario through the same pipeline the real run uses, with every table, topic and
checkpoint carrying the scenario's name, and then asks `chaos.verify` whether the build did what
the scenario said it would.

**A namespace per arm, and nothing dropped between them.** A suite that reuses one set of tables
can only ever tell you about the last thing it ran. Here `dropped_event_silver.charges` and
`baseline_silver.charges` are both in the catalog when the suite finishes, so a difference of nine
rows is a query rather than a claim, and the damaged run is still there to be read.

**Two of the steps are expected to fail, and failure is not the verdict.** Four of the six
scenarios exist to make an invariant fail, so `dbt build` returning a non-zero exit code is the
normal outcome and the run continues past it. What the build actually did is in `run_results.json`,
which is the artifact the verdict is read from. Reading it from an exit code would collapse "the
right test failed on the right rows" into "something happened".

The steps are a list of commands rather than a shell script for one reason: they can be inspected
without running them. `tests/test_chaos_run.py` checks that every table an arm touches carries the
arm's prefix, which is the mistake that would otherwise turn a chaos suite into six runs of the
same undamaged pipeline agreeing with each other.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from payment_ledger import chaos, config

COMPOSE = ["docker", "compose", "-f", "docker/docker-compose.yml"]
JOBS = "/opt/payment-ledger/jobs"
CHECKPOINTS = "/opt/payment-ledger/checkpoints"
MOUNT = "/opt/payment-ledger/data/chaos"
CATALOG = "lakehouse"
LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Step:
    """One thing an arm does, described rather than spelled as a command line.

    Two callers build a command out of this and they build different ones. Here it becomes
    `docker compose run`, because that is what a person at a terminal has. In the Airflow DAG it
    becomes a container the scheduler starts directly, because there is no shell in a task. Naming
    the service, the arguments and the environment instead of the command line is what lets both
    exist without one of them drifting into a second definition of the pipeline.
    """

    label: str
    service: str
    command: list[str]
    env: dict[str, str] = field(default_factory=dict)
    # The service's own entrypoint is what runs a job. A reset runs a shell instead, because what
    # it removes is files and catalog entries rather than anything the pipeline does.
    entrypoint: str | None = None
    # dbt's own contract: 1 is a build that ran and found failures, 2 is a build that never got
    # going (no connection, a broken profile, a compile error). Only the first is an outcome.
    tolerate: bool = False
    capture: bool = False


def topic(scenario: str) -> str:
    return f"stripe.events.{scenario}"


def namespace(scenario: str, layer: str) -> str:
    return f"{CATALOG}.{scenario}_{layer}"


def spark(label: str, job: str, *args: str, **kwargs) -> Step:
    return Step(label, "spark", [f"{JOBS}/{job}", *args], **kwargs)


def compose_argv(step: Step) -> list[str]:
    """The step as the command a terminal runs. `-T` because none of this needs a tty, and a
    captured stream with terminal control codes in it is not a stream anything can parse."""
    flags: list[str] = []
    for key, value in step.env.items():
        flags += ["-e", f"{key}={value}"]
    if step.entrypoint:
        flags += ["--entrypoint", step.entrypoint]
    return [*COMPOSE, "run", "--rm", "-T", *flags, step.service, *step.command]


def wave_steps(scenario: str, wave: str) -> list[Step]:
    """Everything one delivery of one arm does, from the topic to the reported balance.

    The same five jobs the ordinary run submits, pointed at this arm's tables. A wave is delivered
    on top of whatever the previous one left: bronze appends, silver merges, and the entity
    projection is recomputed, which is exactly what has to happen for a late arrival to change a
    day that already closed.
    """
    files = f"{MOUNT}/{scenario}/{wave}"
    return [
        spark(
            f"{wave}: bronze",
            "bronze_events.py",
            "--topic",
            topic(scenario),
            "--table",
            f"{namespace(scenario, 'bronze')}.events",
            "--checkpoint",
            f"{CHECKPOINTS}/{scenario}/bronze",
        ),
        spark(
            f"{wave}: silver events",
            "silver_events.py",
            "--source",
            f"{namespace(scenario, 'bronze')}.events",
            "--table",
            f"{namespace(scenario, 'silver')}.events",
            "--checkpoint",
            f"{CHECKPOINTS}/{scenario}/silver",
        ),
        spark(
            f"{wave}: silver entities",
            "silver_entities.py",
            "--source",
            f"{namespace(scenario, 'silver')}.events",
            "--namespace",
            namespace(scenario, "silver"),
        ),
        spark(
            f"{wave}: balance transaction list",
            "balance_transaction_list.py",
            "--source",
            f"{files}/balance_transactions.jsonl",
            "--table",
            f"{namespace(scenario, 'silver')}.balance_transaction_list",
        ),
        spark(
            f"{wave}: reported balance",
            "reported_balance.py",
            "--source",
            f"{files}/daily_balance.jsonl",
            "--table",
            f"{namespace(scenario, 'silver')}.reported_balance",
        ),
    ]


def dbt_step(scenario: str) -> Step:
    """The ledger, built against this arm's silver and into this arm's gold.

    dbt-spark addresses one schema, so the target is an environment variable and the sources it
    reads are a project variable. Neither is a credential and neither is written to the tree: the
    profile itself is rendered inside the container at start, because a file called `profiles.yml`
    is one this repository will not track.
    """
    return Step(
        label="gold",
        service="dbt",
        command=["build", "--vars", json.dumps({"silver_schema": f"{scenario}_silver"})],
        env={"DBT_SCHEMA": f"{scenario}_gold"},
        # Four of the six scenarios exist to make an invariant fail, so a non-zero exit here is
        # the expected outcome and not the verdict.
        tolerate=True,
    )


def report_step(scenario: str) -> Step:
    return spark("report", "chaos_report.py", "--prefix", f"{scenario}_", capture=True)


def run(step: Step) -> str:
    print(f"\n--- {step.label}")
    result = subprocess.run(
        compose_argv(step),
        cwd=config.REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE if step.capture else None,
        check=False,
    )
    if step.capture:
        print(result.stdout)
    if result.returncode != 0 and not (step.tolerate and result.returncode == 1):
        raise SystemExit(f"{step.label} failed with {result.returncode}")
    return result.stdout or ""


def arm(scenario: str, out: Path, seed: int, source: Path) -> int:
    """One scenario, injected and run and judged. Returns 0 when it did what it said it would."""
    print(f"\n{'=' * 96}\n### {scenario}\n{'=' * 96}")
    manifest = chaos.build(scenario, source, out, seed)
    print(f"injected  {json.dumps(manifest['injected'], sort_keys=True)}")

    create_topic(scenario)

    # The ledger is built after every wave, not once at the end. A late arrival is only a late
    # arrival if the day was closed before it turned up, and a wave that was never closed over is
    # just a slow delivery. What gets judged is the last build, because that is the state the
    # pipeline is actually left in.
    for number in range(1, len(manifest["waves"]) + 1):
        wave = f"wave{number}"
        publish(scenario, out / wave / "events.jsonl")
        for step in wave_steps(scenario, wave):
            run(step)
        forget_run_results()
        run(dbt_step(scenario))

    keep_run_results(out, scenario)
    keep_report(run(report_step(scenario)), out)
    return verdict(scenario, out)


def create_topic(scenario: str) -> None:
    """The arm's own topic, created rather than left to the broker.

    Auto-creation is off, so a typo in a topic name is an error instead of a silently empty stream,
    which means every arm has to make its own. Through the admin API rather than by running
    `kafka-topics.sh` inside the broker's container: the scheduler that runs this DAG has no docker
    CLI and cannot exec into a running service, and a step that only one of the two runners can
    take is a step the other one quietly skips.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap()})
    name = topic(scenario)
    if name in admin.list_topics(timeout=30).topics:
        return

    print(f"\n--- creating {name}")
    admin.create_topics([NewTopic(name, num_partitions=6, replication_factor=1)])[name].result(60)


RUN_RESULTS = config.REPO_ROOT / "dbt" / "target" / "run_results.json"


def forget_run_results() -> None:
    """The last build's record, gone before the next build starts.

    dbt overwrites its artifact when it runs and leaves it alone when it does not. A build that
    never connected used to leave the previous one's record in place, and the verdict then judged a
    build that was not this arm's: red, on tests this scenario never touched. With the file gone, a
    build that did not run leaves nothing, and nothing is something the verdict refuses.
    """
    RUN_RESULTS.unlink(missing_ok=True)


def keep_run_results(out: Path, scenario: str) -> None:
    """dbt overwrites its artifact on every build, so the arm keeps a copy of its own.

    Only once it is known to be this arm's. The build reads the arm's silver through a project
    variable, and dbt records the variables it was given, so a record whose `silver_schema` names
    another arm is another arm's build.
    """
    if not RUN_RESULTS.exists():
        raise SystemExit(
            f"dbt wrote no run_results.json for {scenario}, so its last build never ran. "
            "There is nothing to judge, and judging an older record would be worse than nothing."
        )
    record = json.loads(RUN_RESULTS.read_text(encoding="utf-8"))
    read = (record.get("args") or {}).get("vars", {}).get("silver_schema")
    if read != f"{scenario}_silver":
        raise SystemExit(
            f"run_results.json is the record of a build that read {read}, not {scenario}_silver."
        )
    shutil.copy(RUN_RESULTS, out / "run_results.json")


def keep_report(output: str, out: Path) -> None:
    marker = [line for line in output.splitlines() if line.startswith(chaos.MARKER)]
    if not marker:
        raise SystemExit("the report job printed no measurements, so there is nothing to judge")
    (out / "report.json").write_text(marker[-1][len(chaos.MARKER) :].strip() + "\n", "utf-8")


def publish(scenario: str, events: Path) -> None:
    """The producer runs on the host, the way it does for an ordinary run."""
    from payment_ledger.producer import publish as produce

    print(f"\n--- publishing {events.name}")
    counts = produce(events, bootstrap=bootstrap(), topic=topic(scenario))
    print(f"{sum(counts.values())} events onto {topic(scenario)}")


def bootstrap() -> str:
    return config.setting("KAFKA_BOOTSTRAP", "localhost:9092") or "localhost:9092"


def verdict(scenario: str, out: Path) -> int:
    manifest = json.loads((out / "scenario.json").read_text(encoding="utf-8"))
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    base_path = out.parent / "baseline" / "report.json"
    base = json.loads(base_path.read_text(encoding="utf-8")) if base_path.exists() else {}
    failures = chaos.dbt_failures(out / "run_results.json")

    problems = chaos.verify(manifest, report, failures, base)
    print(f"\n### {scenario}: {manifest['failure']}")
    print(f"  caught by  {manifest['detection']}")
    for name, rows in sorted(failures.items()):
        print(f"  dbt failed {name} on {rows} rows")
    if problems:
        print("  verdict    not the run the scenario described")
        for problem in problems:
            print(f"    {problem}")
        return 1
    print("  verdict    as described")
    return 0


def drop_topics(scenarios: list[str]) -> None:
    """The arms' topics, and then a wait for them to actually be gone.

    Deletion is asynchronous: the broker acknowledges the request and removes the partitions in its
    own time, so creating the topic again immediately afterwards can land on the corpse of the old
    one and inherit its events. The suite does exactly that, one command later.
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap()})
    names = [topic(scenario) for scenario in scenarios]
    existing = [name for name in names if name in admin.list_topics(timeout=30).topics]
    if not existing:
        return

    print(f"\n--- dropping {' '.join(existing)}")
    for name, future in admin.delete_topics(existing, operation_timeout=60).items():
        future.result(60)
        print(f"  {name}")

    for _ in range(30):
        if not set(existing) & set(admin.list_topics(timeout=30).topics):
            return
        time.sleep(1)
    raise SystemExit("the broker still lists a topic that was deleted, so a rerun would resume it")


def inventory() -> list[tuple[str, str]]:
    """Every table an arm can create, as (layer, table), read off the ordinary reset.

    Not a list kept here. `make reset` already names every table the jobs and the models produce,
    and two tests keep that list honest: one against the tables the jobs create, one against the
    gold models. Reading it means a model added tomorrow is dropped by the chaos reset for free,
    and a table left behind by a scenario is the failure mode that makes the next suite compare a
    fresh run against a stale one.

    `DROP NAMESPACE ... CASCADE` would be the obvious way to do this and does not work. Spark
    passes the cascade through to the catalog, the Iceberg REST catalog ignores it, and the drop
    comes back as `NamespaceNotEmptyException: Contains 1 table(s)`.
    """
    makefile = (config.REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    found = re.findall(rf"DROP TABLE IF EXISTS {CATALOG}\.(\w+)\.(\w+)", makefile)
    if not found:
        raise SystemExit("the Makefile names no tables to drop, so a reset would leave everything")
    return sorted(set(found))


def reset(scenarios: list[str], out: Path) -> None:
    """Everything the suite wrote, in the four places it wrote it.

    The same four the ordinary reset has to deal with, per arm: the topic still holds the events,
    the checkpoint still says they were consumed, the catalog still lists the tables, and the
    bucket still holds their files. Forgetting any one of them makes the next suite lie, and the
    quietest of the four is the checkpoint, because a stream that believes it already consumed the
    topic reports an empty run rather than an error.
    """
    drop_topics(scenarios)
    for scenario in scenarios:
        run(
            Step(
                label=f"drop checkpoints {scenario}",
                service="spark",
                entrypoint="/bin/sh",
                command=["-c", f"rm -rf {CHECKPOINTS}/{scenario}"],
            )
        )

    drops = " ".join(
        [
            f"DROP TABLE IF EXISTS {CATALOG}.{s}_{layer}.{table};"
            for s in scenarios
            for layer, table in inventory()
        ]
        + [
            f"DROP NAMESPACE IF EXISTS {namespace(s, layer)};"
            for s in scenarios
            for layer in LAYERS
        ]
    )
    run(
        Step(
            label="drop tables and namespaces",
            service="spark",
            entrypoint="/opt/spark/bin/spark-sql",
            command=["-e", drops],
        )
    )

    prefixes = " ".join(f"l/warehouse/{s}_{layer}" for s in scenarios for layer in LAYERS)
    run(
        Step(
            label="drop files",
            service="minio-init",
            entrypoint="/bin/sh",
            command=[
                "-c",
                "mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && "
                # The files are removed where they live rather than with `DROP TABLE ... PURGE`,
                # which against MinIO logs a failure per object and leaves them behind.
                f"mc rm --recursive --force --quiet {prefixes} || true",
            ],
        )
    )
    for scenario in scenarios:
        if (out / scenario).exists():
            shutil.rmtree(out / scenario)


def clear(scenario: str, out: Path) -> None:
    """Take one arm back to nothing while keeping what it found.

    The difference from a reset is the three files the verdict was read from: what this drops is
    the lakehouse, the topic and the damaged stream on disk, which together are the whole cost of
    an arm, while `scenario.json`, `report.json` and `run_results.json` stay. Running the suite one
    arm at a time is what makes a large run possible at all, and it costs the side by side
    comparison the ordinary suite leaves in the catalog.
    """
    findings = {}
    for keep in ("scenario.json", "report.json", "run_results.json"):
        path = out / scenario / keep
        if path.exists():
            findings[keep] = path.read_bytes()

    reset([scenario], out)

    if findings:
        (out / scenario).mkdir(parents=True, exist_ok=True)
        for name, content in findings.items():
            (out / scenario / name).write_bytes(content)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the chaos suite and judge every arm.")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=sorted(chaos.SCENARIOS),
        default=None,
        help="run these arms instead of all of them",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="drop each arm once it has been judged, keeping only what it found. The suite"
        " otherwise leaves all seven lakehouses side by side, which is worth having and is"
        " what makes a large run impossible on a small disk",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=config.REPO_ROOT / "data" / "generated",
        help="the generated run to damage. The suite runs the pipeline once per scenario, so this"
        " is affordable at a size the ledger itself may not be (default: %(default)s)",
    )
    parser.add_argument("--out", type=Path, default=config.REPO_ROOT / "data" / "chaos")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="drop what a previous suite wrote and stop",
    )
    args = parser.parse_args(argv)

    # A suite arm takes minutes and the containers it starts write straight to the same stream, so
    # a block-buffered stdout puts this process's own account of what it is doing thousands of
    # lines behind theirs. Redirected to a file, that is the difference between a log you can watch
    # and a log you can only read afterwards.
    sys.stdout.reconfigure(line_buffering=True)

    # The control arm first, because every other arm is measured against its counts.
    order = ["baseline"] + [name for name in chaos.SCENARIOS if name != "baseline"]
    scenarios = [name for name in order if args.only is None or name in args.only]

    if args.reset:
        # `--only` narrows a reset the same way it narrows a run, so one arm can be taken back to
        # nothing without throwing away the baseline every other arm is measured against.
        reset(scenarios, args.out)
        return 0

    failed = []
    for scenario in scenarios:
        if arm(scenario, args.out / scenario, args.seed, args.source) != 0:
            failed.append(scenario)
        if args.sequential:
            # `clear` and not `reset`: what is dropped is the lakehouse, the topic and the damaged
            # stream, and what stays is the three files the verdict was read from. A reset here
            # would take the finding with the evidence and leave the suite with nothing to export.
            #
            # The control arm is cleared like any other. What the later arms compare themselves
            # against is its `report.json`, which is one of the files kept, and exempting it cost
            # this run a disk: its stream and its topic are eighteen gigabytes nothing reads again.
            print(f"\n--- clearing {scenario} before the next arm")
            clear(scenario, args.out)

    print(f"\n{'=' * 96}")
    if failed:
        print(f"arms that did not do what they said they would: {' '.join(failed)}")
        return 1
    print(f"{len(scenarios)} of them: every failure was caught, and nothing else fired")
    return 0


if __name__ == "__main__":
    sys.exit(main())
