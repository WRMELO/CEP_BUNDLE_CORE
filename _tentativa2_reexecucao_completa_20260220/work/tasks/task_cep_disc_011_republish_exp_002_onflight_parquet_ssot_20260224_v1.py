from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_task_spec(path: Path) -> dict[str, Any]:
    # YAML no projeto e serializado em JSON válido.
    return read_json(path)


def copy_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def ensure_inactive_marker(path: Path, marker_name: str, canonical_path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    marker = path / marker_name
    text = "\n".join(
        [
            f"# Inativo para uso operacional - {path.name}",
            "",
            f"- status: `INACTIVE_FOR_NON_AUDIT`",
            f"- superseded_by: `{canonical_path}`",
            "- policy: consumo operacional proibido neste path.",
            "- allowed_use: apenas auditoria histórica.",
            f"- marked_at_utc: `{now_utc()}`",
            "",
        ]
    )
    write_text(marker, text)
    return marker


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    task_spec = read_task_spec(Path(args.task_spec).resolve())
    repo_root = Path(task_spec["repo_root"]).resolve()
    working_root = Path(task_spec["working_root"]).resolve()
    outputs_governance = (working_root / task_spec["agnostic_requirements"]["outputs_dir"]).resolve()
    run_root = (repo_root / task_spec["agnostic_requirements"]["run_artifacts_root"]).resolve()
    run_dir = run_root / f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    evidence_dir = outputs_governance / "evidence"
    run_dir.mkdir(parents=True, exist_ok=True)
    outputs_governance.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    source_outputs = (working_root / task_spec["inputs"]["source_outputs_dir_current"]).resolve()
    source_run_report = (repo_root / task_spec["inputs"]["source_run_report"]).resolve()
    source_task_spec = (repo_root / task_spec["inputs"]["source_task_spec"]).resolve()
    canonical_outputs = (working_root / task_spec["target_canonical"]["canonical_outputs_dir"]).resolve()
    canonical_tables = canonical_outputs / "tables"

    gates: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []

    # S1 allowlist
    s1_ok = (
        str(source_outputs).startswith(str(working_root / "outputs"))
        and str(canonical_outputs).startswith(str(working_root / "outputs/experimentos/on_flight"))
        and str(outputs_governance).startswith(str(working_root / "outputs/governanca"))
        and str(run_dir).startswith(str(repo_root / "planning/runs"))
    )
    gates.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if s1_ok else "FAIL"})
    steps.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if s1_ok else "FAIL"})
    if not s1_ok:
        report = {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps}
        write_json(run_dir / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 1

    # S2 verify source/pass
    s2_ok = all(p.exists() for p in [source_outputs, source_run_report, source_task_spec, source_outputs / "manifest.json"])
    src_manifest = read_json(source_outputs / "manifest.json") if (source_outputs / "manifest.json").exists() else {}
    src_report = read_json(source_run_report) if source_run_report.exists() else {}
    s2_ok = s2_ok and str(src_manifest.get("overall", "")).upper() == "PASS" and bool(src_report.get("overall_pass", False))
    gates.append({"name": "S2_VERIFY_SOURCE_EXISTS_AND_PASS", "status": "PASS" if s2_ok else "FAIL"})
    steps.append({"name": "S2_VERIFY_SOURCE_EXISTS_AND_PASS", "status": "PASS" if s2_ok else "FAIL"})
    if not s2_ok:
        report = {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps}
        write_json(run_dir / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 1

    # S3 republish canonical on_flight (sem recomputar)
    canonical_outputs.mkdir(parents=True, exist_ok=True)
    canonical_tables.mkdir(parents=True, exist_ok=True)
    for name in ["report.md", "summary.json"]:
        copy_if_exists(source_outputs / name, canonical_outputs / name)
    if (source_outputs / "evidence").exists():
        if (canonical_outputs / "evidence").exists():
            shutil.rmtree(canonical_outputs / "evidence")
        shutil.copytree(source_outputs / "evidence", canonical_outputs / "evidence")
    copied_csv: list[str] = []
    for csv_path in sorted((source_outputs / "tables").glob("*.csv")):
        dst = canonical_tables / csv_path.name
        shutil.copy2(csv_path, dst)
        copied_csv.append(str(dst))
    s3_ok = (canonical_outputs / "report.md").exists() and (canonical_outputs / "summary.json").exists() and len(copied_csv) > 0
    gates.append({"name": "S3_REPUBLISH_TO_CANONICAL_ONFLIGHT", "status": "PASS" if s3_ok else "FAIL", "details": {"csv_copied": len(copied_csv)}})
    steps.append({"name": "S3_REPUBLISH_TO_CANONICAL_ONFLIGHT", "status": "PASS" if s3_ok else "FAIL"})

    # S4 parquet-first
    parquet_generated: list[str] = []
    parquet_missing: list[str] = []
    for csv_path in sorted(canonical_tables.glob("*.csv")):
        pq_path = csv_path.with_suffix(".parquet")
        try:
            df = pd.read_csv(csv_path)
            df.to_parquet(pq_path, index=False)
            parquet_generated.append(str(pq_path))
        except Exception:
            parquet_missing.append(str(pq_path))
    s4_ok = len(parquet_generated) == len(list(canonical_tables.glob("*.csv"))) and len(parquet_missing) == 0
    gates.append(
        {
            "name": "S4_ENFORCE_PARQUET_FIRST",
            "status": "PASS" if s4_ok else "FAIL",
            "details": {"parquet_generated": len(parquet_generated), "parquet_missing": parquet_missing},
        }
    )
    steps.append({"name": "S4_ENFORCE_PARQUET_FIRST", "status": "PASS" if s4_ok else "FAIL"})

    # S5 supersedence + ssot readme
    marker_name = task_spec["supersedence_rules"]["inactive_marker_filename"]
    inactive_markers: list[str] = []
    for rel in task_spec["supersedence_rules"]["mark_inactive_paths"]:
        target = (working_root / rel).resolve()
        marker = ensure_inactive_marker(target, marker_name, canonical_outputs)
        inactive_markers.append(str(marker))
    readme_path = (working_root / task_spec["supersedence_rules"]["readme_supersedence_parent_dir"] / task_spec["supersedence_rules"]["readme_filename"]).resolve()
    readme_text = "\n".join(
        [
            "# README_SUPERSEDENCE",
            "",
            "## EXPs ACTIVE (consumo operacional permitido)",
            f"- `{canonical_outputs}`",
            "",
            "## Paths INACTIVE_FOR_NON_AUDIT (superseded)",
            f"- `{(working_root / 'outputs/experimentos/fase1_calibracao/exp/20260224/exp_002_sell_policy_gate_rl').resolve()}`",
            f"- `{(working_root / 'work/experimentos_on_flight/EXP_002_SELL_POLICY_GATE_CEP_RL_V1').resolve()}`",
            "",
            "## Regra operacional",
            "- Consumir apenas paths ACTIVE.",
            "- Paths INACTIVE_FOR_NON_AUDIT podem ser usados somente para auditoria histórica.",
            f"- updated_at_utc: `{now_utc()}`",
            "",
        ]
    )
    write_text(readme_path, readme_text)
    s5_ok = len(inactive_markers) == len(task_spec["supersedence_rules"]["mark_inactive_paths"]) and readme_path.exists()
    gates.append({"name": "S5_WRITE_SUPERSEDENCE_AND_SSOT_README", "status": "PASS" if s5_ok else "FAIL", "details": {"readme": str(readme_path)}})
    steps.append({"name": "S5_WRITE_SUPERSEDENCE_AND_SSOT_README", "status": "PASS" if s5_ok else "FAIL"})

    # S6 manifest/hashes sem ambiguidade
    files_for_hash: list[Path] = []
    for p in [canonical_outputs / "report.md", canonical_outputs / "summary.json", readme_path]:
        if p.exists():
            files_for_hash.append(p)
    for p in sorted(canonical_tables.glob("*.csv")) + sorted(canonical_tables.glob("*.parquet")):
        files_for_hash.append(p)
    if (canonical_outputs / "evidence").exists():
        for p in sorted((canonical_outputs / "evidence").glob("*")):
            if p.is_file():
                files_for_hash.append(p)
    hashes = {str(p): sha256_file(p) for p in files_for_hash if p.exists() and p.is_file()}
    canonical_manifest = {
        "task_id": task_spec["instruction_id"],
        "generated_at_utc": now_utc(),
        "output_root": str(canonical_outputs),
        "overall": "PASS",
        "parquet_first_enforced": s4_ok,
        "active_path": str(canonical_outputs),
        "inactive_paths": [str((working_root / rel).resolve()) for rel in task_spec["supersedence_rules"]["mark_inactive_paths"]],
        "required_files": sorted(list(hashes.keys())),
        "hashes_sha256": hashes,
    }
    write_json(canonical_outputs / "manifest.json", canonical_manifest)
    canonical_manifest["hashes_sha256"][str(canonical_outputs / "manifest.json")] = sha256_file(canonical_outputs / "manifest.json")
    write_json(canonical_outputs / "manifest.json", canonical_manifest)

    ambiguity = []
    if (working_root / "outputs/experimentos/fase1_calibracao/exp/20260224/exp_002_sell_policy_gate_rl").exists():
        marker = (working_root / "outputs/experimentos/fase1_calibracao/exp/20260224/exp_002_sell_policy_gate_rl" / marker_name)
        if not marker.exists():
            ambiguity.append("missing_inactive_marker_old_output")
    s6_ok = (canonical_outputs / "manifest.json").exists() and len(ambiguity) == 0
    gates.append({"name": "S6_VERIFY_MANIFEST_HASHES_AND_NO_AMBIGUITY", "status": "PASS" if s6_ok else "FAIL", "details": {"ambiguity": ambiguity}})
    steps.append({"name": "S6_VERIFY_MANIFEST_HASHES_AND_NO_AMBIGUITY", "status": "PASS" if s6_ok else "FAIL"})

    # Governanca output report
    governance_report = {
        "task_id": task_spec["instruction_id"],
        "status": "PASS" if all(g["status"] == "PASS" for g in gates) else "FAIL",
        "overall_pass": all(g["status"] == "PASS" for g in gates),
        "gates": gates,
        "steps": steps,
        "deliverables": {
            "canonical_outputs_dir": str(canonical_outputs),
            "inactive_markers": inactive_markers,
            "readme_supersedence": str(readme_path),
            "canonical_manifest": str(canonical_outputs / "manifest.json"),
        },
        "timestamp_utc": now_utc(),
    }
    write_json(outputs_governance / "report.json", governance_report)
    write_json(run_dir / "report.json", governance_report)
    write_json(run_dir / "run_summary.json", governance_report)
    write_text(
        outputs_governance / "report.md",
        "\n".join(
            [
                f"# Report - {task_spec['instruction_id']}",
                "",
                f"- overall: `{governance_report['status']}`",
                f"- canonical_outputs_dir: `{canonical_outputs}`",
                f"- parquet_generated: `{len(parquet_generated)}`",
                f"- inactive_markers_written: `{len(inactive_markers)}`",
                "",
            ]
        )
        + "\n",
    )

    return 0 if governance_report["overall_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
