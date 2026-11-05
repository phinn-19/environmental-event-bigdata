"""Package six verified Spark outputs for immutable HF publication."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil
from pathlib import PurePosixPath

TABLES = {
    "silver": ("air_quality_hourly", "fire_hotspots", "weather_hourly"),
    "curated": ("air_quality_baseline", "air_quality_context_hourly", "fire_hotspots_context"),
}


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate(p):
    p = p.resolve()
    manifest = p / "SHA256SUMS.txt"
    if not manifest.is_file():
        raise FileNotFoundError("Missing SHA256SUMS.txt")
    expected = set()
    for line in manifest.read_text(encoding="utf-8").splitlines():
        digest, path = line.split("  ", 1)
        rel = PurePosixPath(path)
        if (not re.fullmatch(r"[0-9a-f]{64}", digest) or not path or
                rel.is_absolute() or rel.as_posix() != path or "\\" in path or
                any(part in ("..", ".") for part in rel.parts) or
                path == "SHA256SUMS.txt" or path in expected):
            raise ValueError(f"Unsafe/duplicate checksum entry: {path}")
        original = p.joinpath(*rel.parts)
        file = original.resolve()
        is_link = any(p.joinpath(*rel.parts[:i]).is_symlink() for i in range(1, len(rel.parts) + 1))
        if (not file.is_relative_to(p) or is_link or
                not file.is_file() or sha(file) != digest):
            raise ValueError(f"Invalid file in package: {path}")
        expected.add(path)
    actual = {x.relative_to(p).as_posix() for x in p.rglob("*") if x.is_file()}
    if not expected or actual != expected | {"SHA256SUMS.txt"}:
        raise ValueError("Package file inventory mismatch")
    return len(actual)


def build(job: Path, package: Path, run_id: str, commit: str):
    if package.exists():
        raise FileExistsError("Refusing to overwrite package")
    if not re.fullmatch(r"run_[A-Za-z0-9_-]{4,90}", run_id):
        raise ValueError("Unsafe run ID")
    if not (job / "SELECTION.json").is_file() or not (job / "SCOPE_REPORT.json").is_file():
        raise FileNotFoundError("Missing verified selection / daily scope metadata")
    scope_info = json.loads((job / "SCOPE_REPORT.json").read_text(encoding="utf-8"))
    if set(scope_info.get("tables", {})) != set(sum((list(x) for x in TABLES.values()), [])):
        raise ValueError("Missing scoped table report for six tables")
    for tier, names in TABLES.items():
        for table in names:
            source = job / "data" / tier / table
            if not (source / "_SUCCESS").is_file() or not list(source.glob("*.parquet")):
                raise ValueError(f"Incomplete output: {tier}/{table}")
    package.mkdir(parents=True)
    for tier, names in TABLES.items():
        for table in names:
            target = package / tier / table
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(job / "data" / tier / table, target)
    shutil.copy2(job / "SELECTION.json", package / "SELECTION.json")
    shutil.copy2(job / "SCOPE_REPORT.json", package / "SCOPE_REPORT.json")
    (package / "PROVENANCE.json").write_text(json.dumps({
        "run_id": run_id, "git_commit": commit,
        "source": "hf://buckets/CourseworkBigData/env-event-lake/lake/v1/data/raw",
        "scope": "bounded_manual_batch; outputs strictly filtered to each source target UTC day",
        "processing_tables": TABLES,
    }, indent=2) + "\n", encoding="utf-8")
    (package / "README.md").write_text(
        f"# Processing batch {run_id}\n\nGit commit `{commit}`.\n"
        "Data derived from verified HF Raw; selection and daily scope recorded in JSON files. "
        "This is a bounded batch, NOT an end-to-end daily incremental pipeline. "
        "Null context and unusable baseline are possible.\n", encoding="utf-8")
    with (package / "INVENTORY.csv").open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(("path", "bytes", "sha256"))
        for f in sorted(package.rglob("*")):
            if f.is_file():
                w.writerow((f.relative_to(package).as_posix(), f.stat().st_size, sha(f)))
    lines = [f"{sha(f)}  {f.relative_to(package).as_posix()}" for f in sorted(package.rglob("*")) if f.is_file()]
    (package / "SHA256SUMS.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"PACKAGE VERIFIED: {validate(package)} files")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--job", type=Path)
    p.add_argument("--package", type=Path, required=True)
    p.add_argument("--run-id")
    p.add_argument("--commit")
    p.add_argument("--verify-only", action="store_true")
    a = p.parse_args()
    if a.verify_only:
        print(f"PACKAGE VERIFIED: {validate(a.package)} files")
    else:
        if not (a.job and a.run_id and a.commit):
            p.error("--job --run-id --commit required")
        build(a.job, a.package, a.run_id, a.commit)


if __name__ == "__main__":
    main()
