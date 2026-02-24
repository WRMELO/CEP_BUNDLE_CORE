from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


OFFICIAL_PYTHON = "/home/wilson/PortfolioZero/.venv/bin/python"
PREVIOUS_INSTRUCTION_ID = "TASK_CEP_EXP_002_SELL_POLICY_GATE_CEP_RL_V1"
CURRENT_INSTRUCTION_ID = "TASK_CEP_F1_EXP_032_SELL_POLICY_GATE_CEP_RL_V1"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def read_task_spec(task_spec_path: Path) -> dict[str, Any]:
    raw = task_spec_path.read_text(encoding="utf-8")
    # O task spec em .yaml aqui e serializado em JSON valido para evitar dependencia externa.
    return json.loads(raw)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def run_cmd(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=str(cwd), text=True, capture_output=True, check=False)


def mark_inactive(path: Path, reason: str, evidence: list[str]) -> None:
    if not path.exists():
        return
    marker = path / "INACTIVE_FOR_NON_AUDIT.md"
    content = "\n".join(
        [
            f"# Inativo para uso operacional - {path.name}",
            "",
            f"- previous_instruction_id: `{PREVIOUS_INSTRUCTION_ID}`",
            f"- superseded_by_instruction_id: `{CURRENT_INSTRUCTION_ID}`",
            "- status: `INACTIVE_FOR_NON_AUDIT`",
            "- allowed_use: `AUDITORIA_ONLY`",
            f"- reason: {reason}",
            f"- marked_at_utc: `{now_utc()}`",
            "",
            "## Evidencias",
            *[f"- `{x}`" for x in evidence],
            "",
        ]
    )
    write_text(marker, content)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    task_spec_path = Path(args.task_spec).resolve()
    task_spec = read_task_spec(task_spec_path)

    repo_root = Path(task_spec["repo_root"]).resolve()
    working_root = Path(task_spec["working_root"]).resolve()
    outputs_dir = (working_root / task_spec["agnostic_requirements"]["outputs_dir"]).resolve()
    run_root = (repo_root / task_spec["agnostic_requirements"]["run_artifacts_root"]).resolve()
    run_dir = run_root / f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    evidence_dir = run_dir / "evidence"
    run_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    gates: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    report_path = run_dir / "report.json"

    exp_script = working_root / "work/experimentos_on_flight/EXP_002_SELL_POLICY_GATE_CEP_RL_V1/run_experiment.py"
    exp_base_config = working_root / "work/experimentos_on_flight/EXP_002_SELL_POLICY_GATE_CEP_RL_V1/experiment_config.json"
    effective_config_path = evidence_dir / "effective_experiment_config.json"

    # S1_GATE_ALLOWLIST_PATHS
    allow_ok = (
        str(outputs_dir).startswith(str(working_root / "outputs"))
        and str(run_dir).startswith(str(repo_root / "planning/runs"))
        and str(exp_script).startswith(str(working_root / "work"))
    )
    gates.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if allow_ok else "FAIL", "details": {"outputs_dir": str(outputs_dir), "run_dir": str(run_dir)}})
    steps.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if allow_ok else "FAIL"})
    if not allow_ok:
        write_json(report_path, {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps, "error": "allowlist failed"})
        return 1

    # S2_CHECK_COMPILE_OR_IMPORT
    comp = run_cmd([OFFICIAL_PYTHON, "-m", "py_compile", str(exp_script)], cwd=repo_root)
    write_text(evidence_dir / "compile_check.txt", f"rc={comp.returncode}\nstdout={comp.stdout}\nstderr={comp.stderr}\n")
    s2_ok = comp.returncode == 0
    gates.append({"name": "S2_CHECK_COMPILE_OR_IMPORT", "status": "PASS" if s2_ok else "FAIL"})
    steps.append({"name": "S2_CHECK_COMPILE_OR_IMPORT", "status": "PASS" if s2_ok else "FAIL"})
    if not s2_ok:
        write_json(report_path, {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps, "error": "compile failed"})
        return 1

    # S3_RUN_EXPERIMENT
    cfg = load_json(exp_base_config)
    cfg["task_id"] = task_spec["instruction_id"]
    cfg["paths"]["outputs_root"] = str(outputs_dir)
    write_json(effective_config_path, cfg)
    runp = run_cmd([OFFICIAL_PYTHON, str(exp_script), "--config", str(effective_config_path)], cwd=repo_root)
    write_text(evidence_dir / "run_experiment_stdout.txt", runp.stdout)
    write_text(evidence_dir / "run_experiment_stderr.txt", runp.stderr)
    s3_ok = runp.returncode == 0
    gates.append({"name": "S3_RUN_EXPERIMENT", "status": "PASS" if s3_ok else "FAIL", "details": {"returncode": runp.returncode}})
    steps.append({"name": "S3_RUN_EXPERIMENT", "status": "PASS" if s3_ok else "FAIL"})
    if not s3_ok:
        write_json(report_path, {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps, "error": "run failed"})
        return 1

    # Supersedencia + inativacao da instrucao anterior (nao sobreposto)
    previous_output_root = working_root / "outputs/experimentos/fase1_calibracao/exp_002_sell_policy_gate_rl"
    previous_work_root = working_root / "work/experimentos_on_flight/EXP_002_SELL_POLICY_GATE_CEP_RL_V1"
    mark_inactive(
        previous_output_root,
        reason="Instrução anterior substituída por TASK_CEP_F1_EXP_032; manter apenas para auditoria histórica.",
        evidence=[str(outputs_dir), str(run_dir / "report.json")],
    )
    mark_inactive(
        previous_work_root,
        reason="Implementação/artefatos de trabalho anteriores inativados para efeito operacional.",
        evidence=[str(exp_script), str(effective_config_path)],
    )

    # S4_VERIFY_NO_LEAKAGE
    leak_path = outputs_dir / "evidence/feature_leakage_audit.json"
    leak_ok = False
    leak_payload: dict[str, Any] = {}
    if leak_path.exists():
        leak_payload = load_json(leak_path)
        leak_ok = bool(
            leak_payload.get("uses_only_same_or_past_data_for_decisions", False)
            and leak_payload.get("expost_label_used_for_metrics_only", False)
            and leak_payload.get("rl_feedback_released_only_at_D_plus_3_or_later", False)
        )
    gates.append({"name": "S4_VERIFY_NO_LEAKAGE", "status": "PASS" if leak_ok else "FAIL", "details": leak_payload})
    steps.append({"name": "S4_VERIFY_NO_LEAKAGE", "status": "PASS" if leak_ok else "FAIL"})

    # S5_VERIFY_OUTPUTS_AND_MANIFEST
    required = [
        outputs_dir / "report.md",
        outputs_dir / "summary.json",
        outputs_dir / "manifest.json",
        outputs_dir / "tables/equity_curve_all.csv",
        outputs_dir / "tables/trades_all.csv",
        outputs_dir / "tables/sell_decisions_all.csv",
        outputs_dir / "tables/metrics_by_regime.csv",
        outputs_dir / "tables/metrics_by_subperiod.csv",
    ]
    exists_ok = all(p.exists() and p.stat().st_size > 0 for p in required)
    manifest_ok = False
    manifest_payload: dict[str, Any] = {}
    if (outputs_dir / "manifest.json").exists():
        manifest_payload = load_json(outputs_dir / "manifest.json")
        manifest_ok = str(manifest_payload.get("overall", "")).upper() == "PASS"
    s5_ok = exists_ok and manifest_ok
    gates.append({"name": "S5_VERIFY_OUTPUTS_AND_MANIFEST", "status": "PASS" if s5_ok else "FAIL", "details": {"exists_ok": exists_ok, "manifest_ok": manifest_ok}})
    steps.append({"name": "S5_VERIFY_OUTPUTS_AND_MANIFEST", "status": "PASS" if s5_ok else "FAIL"})

    # S6_SUMMARIZE_RESULTS_AND_DECISION_NOTES
    summary_path = outputs_dir / "summary.json"
    decision_notes: dict[str, Any] = {}
    s6_ok = False
    if summary_path.exists():
        rows = json.loads(summary_path.read_text(encoding="utf-8"))
        best_cagr = sorted(rows, key=lambda r: float(r.get("CAGR", float("-inf"))), reverse=True)[0]
        best_low_churn = sorted(rows, key=lambda r: float(r.get("turnover_sell", float("inf"))))[0]
        decision_notes = {
            "best_cagr_variant": {"variant": best_cagr.get("variant"), "slope_w": best_cagr.get("slope_w"), "CAGR": best_cagr.get("CAGR"), "MDD": best_cagr.get("MDD")},
            "best_low_churn_variant": {
                "variant": best_low_churn.get("variant"),
                "slope_w": best_low_churn.get("slope_w"),
                "turnover_sell": best_low_churn.get("turnover_sell"),
                "cost_total": best_low_churn.get("cost_total"),
            },
        }
        s6_ok = True
    gates.append({"name": "S6_SUMMARIZE_RESULTS_AND_DECISION_NOTES", "status": "PASS" if s6_ok else "FAIL", "details": decision_notes})
    steps.append({"name": "S6_SUMMARIZE_RESULTS_AND_DECISION_NOTES", "status": "PASS" if s6_ok else "FAIL"})

    overall = all(g["status"] == "PASS" for g in gates)
    report = {
        "task_id": task_spec["instruction_id"],
        "status": "PASS" if overall else "FAIL",
        "overall_pass": overall,
        "supersedes": PREVIOUS_INSTRUCTION_ID,
        "gates": gates,
        "steps": steps,
        "deliverables": {
            "outputs_dir": str(outputs_dir),
            "report_md": str(outputs_dir / "report.md"),
            "summary_json": str(outputs_dir / "summary.json"),
            "metrics_detail_dir": str(outputs_dir / "tables"),
            "manifest_json": str(outputs_dir / "manifest.json"),
            "inactive_marker_previous_output": str(previous_output_root / "INACTIVE_FOR_NON_AUDIT.md"),
            "inactive_marker_previous_work": str(previous_work_root / "INACTIVE_FOR_NON_AUDIT.md"),
            "run_report_json": str(report_path),
        },
        "decision_notes": decision_notes,
        "timestamp_utc": now_utc(),
    }
    write_json(report_path, report)
    write_json(run_dir / "run_summary.json", report)
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
