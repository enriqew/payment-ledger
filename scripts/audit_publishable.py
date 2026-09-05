"""Answer "is this repository safe to publish" as a command instead of as a reading of the diff.

Scans every tracked file against the rules in `payment_ledger.redact`, which are the same rules
the capture applies when it writes a fixture. Two scopes, because they are two different risks:

- A **live credential** is refused anywhere in the tree, with no exceptions. There is no file whose
  job is to contain one.
- An **identifier of the sandbox account** (a test key, a signing secret, an account id, a receipt
  token, an email) is refused in the payloads and in anything the pipeline generates, and allowed
  in the handful of places whose job is to show what one looks like: the tests, the docs, the
  example dotfile. Those exceptions are listed here rather than inferred, so adding one is a
  visible decision.

Exit code 1 on any finding, so `make audit` and CI fail the same way.

The matched text is never printed. Printing a secret to a CI log is how a secret ends up somewhere
worse than the file it was found in, so the report names the file, the line and the rule.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from payment_ledger.redact import FORBIDDEN_ANYWHERE, FORBIDDEN_IN_PAYLOADS  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

# Files whose purpose is to show the shape of a credential rather than to hold one. The account
# identifier rules do not apply to them; the live credential rules still do.
PLACEHOLDER_ALLOWED = (
    "tests/",
    "docs/",
    "scripts/audit_publishable.py",
    "src/payment_ledger/redact.py",
    "README.md",
    "fixtures/README.md",
    ".env.example",
    # The author address is meant to be published: it is the identity on every commit here.
    "pyproject.toml",
    "LICENSE",
)

# Binary and generated files. Reading them as text produces noise, not findings.
SKIP_SUFFIXES = {".png", ".jpg", ".gif", ".ico", ".pdf", ".zip", ".parquet", ".exe"}


def tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [REPO_ROOT / name for name in out.split("\0") if name]


def rules_for(relative: str) -> dict:
    if relative.startswith(PLACEHOLDER_ALLOWED):
        return FORBIDDEN_ANYWHERE
    return {**FORBIDDEN_ANYWHERE, **FORBIDDEN_IN_PAYLOADS}


def scan(path: Path) -> list[tuple[int, str]]:
    relative = path.relative_to(REPO_ROOT).as_posix()
    if path.suffix.lower() in SKIP_SUFFIXES or not path.is_file():
        return []

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return []

    rules = rules_for(relative)
    return [
        (number, name)
        for number, line in enumerate(lines, start=1)
        for name, pattern in rules.items()
        if pattern.search(line)
    ]


def main() -> int:
    findings = []
    files = tracked_files()

    for path in files:
        relative = path.relative_to(REPO_ROOT).as_posix()
        findings += [(relative, number, name) for number, name in scan(path)]

    if not findings:
        print(f"clean: {len(files)} tracked files, no credentials and no account identifiers")
        return 0

    print(f"{len(findings)} finding(s). This repository is public; none of these may be pushed.\n")
    for relative, number, name in findings:
        print(f"  {relative}:{number}  {name}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
