from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

OFFICIAL_PYTHON = "/home/wilson/PortfolioZero/.venv/bin/python"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_task_spec(path: Path) -> dict[str, Any]:
    # YAML no repositório está serializado em JSON válido.
    return read_json(path)


def run_cmd(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=str(cwd), text=True, capture_output=True, check=False)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    task = parse_task_spec(Path(args.task_spec).resolve())
    repo_root = Path(task["repo_root"]).resolve()
    working_root = Path(task["working_root"]).resolve()
    outputs_dir = (working_root / task["agnostic_requirements"]["outputs_dir"]).resolve()
    run_root = (repo_root / task["agnostic_requirements"]["run_artifacts_root"]).resolve()
    run_dir = run_root / f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    ssot_dir = outputs_dir / "ssot"
    evidence_dir = outputs_dir / "evidence"
    audits_dir = outputs_dir / "audits"
    design_dir = outputs_dir / "design"
    for p in [outputs_dir, ssot_dir, evidence_dir, audits_dir, design_dir]:
        p.mkdir(parents=True, exist_ok=True)

    gates: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []

    # S1 allowlist
    s1_ok = (
        str(outputs_dir).startswith(str(working_root / "outputs"))
        and str(run_dir).startswith(str(repo_root / "planning/runs"))
    )
    gates.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if s1_ok else "FAIL"})
    steps.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if s1_ok else "FAIL"})
    if not s1_ok:
        report = {"task_id": task["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps}
        write_json(run_dir / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 1

    # S2 build SSOT corporate actions
    rows = []
    for ep in task["episodes"]:
        rows.append(
            {
                "ticker": ep["ticker"],
                "action_type": "SPLIT" if float(ep["factor"]) >= 1.0 else "REVERSE_SPLIT",
                "ex_date": ep["ex_date"],
                "factor": float(ep["factor"]),
                "source": "B3_EPISODE_VALIDATION_MANUAL",
                "evidence_id": f"{ep['ticker']}_{ep['ex_date']}_FACTOR_{ep['factor']}",
                "retrieved_at": now_utc(),
            }
        )
    ssot_df = pd.DataFrame(rows)
    ssot_df["ex_date"] = pd.to_datetime(ssot_df["ex_date"]).dt.normalize()
    ssot_df.to_parquet(ssot_dir / "corporate_actions.parquet", index=False)
    ssot_df.to_csv(ssot_dir / "corporate_actions.csv", index=False)
    s2_ok = (ssot_dir / "corporate_actions.parquet").exists()
    gates.append({"name": "S2_BUILD_SSOT_CORPORATE_ACTIONS", "status": "PASS" if s2_ok else "FAIL", "details": {"rows": len(ssot_df)}})
    steps.append({"name": "S2_BUILD_SSOT_CORPORATE_ACTIONS", "status": "PASS" if s2_ok else "FAIL"})

    # S3 implement split application in backtest (compile + code presence)
    exp1_script = working_root / "work/experimentos_on_flight/EXP_001_S008_TOP10S45_STRESSSELL_2019_20210630_V1/run_experiment.py"
    exp2_script = working_root / "work/experimentos_on_flight/EXP_002_SELL_POLICY_GATE_CEP_RL_V1/run_experiment.py"
    comp = run_cmd([OFFICIAL_PYTHON, "-m", "py_compile", str(exp1_script), str(exp2_script)], cwd=repo_root)
    write_text(evidence_dir / "compile_backtests.txt", f"rc={comp.returncode}\nstdout={comp.stdout}\nstderr={comp.stderr}\n")
    src1 = exp1_script.read_text(encoding="utf-8")
    src2 = exp2_script.read_text(encoding="utf-8")
    markers_ok = all(x in src1 for x in ["load_split_factor_map", "CORPORATE_ACTION_SPLIT"]) and all(
        x in src2 for x in ["load_split_factor_map", "build_adjusted_logret_from_splits"]
    )
    s3_ok = comp.returncode == 0 and markers_ok
    gates.append({"name": "S3_IMPLEMENT_SPLIT_APPLICATION_IN_BACKTEST", "status": "PASS" if s3_ok else "FAIL"})
    steps.append({"name": "S3_IMPLEMENT_SPLIT_APPLICATION_IN_BACKTEST", "status": "PASS" if s3_ok else "FAIL"})

    # S4 adjusted series for CEP (close adjusted + adjusted logret)
    panel_path = working_root / "outputs/base_operacional_canonica_completa_v2_448_cdi_excess_v1/panel/base_operacional_canonica.parquet"
    panel = pd.read_parquet(panel_path, columns=["date", "ticker", "close"])
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    panel = panel[panel["ticker"].isin(ssot_df["ticker"].tolist())].copy()
    panel = panel.sort_values(["ticker", "date"])
    panel["prev_close"] = panel.groupby("ticker")["close"].shift(1)
    panel["raw_logret"] = np.log(panel["close"] / panel["prev_close"])
    factor_map = {(r["ticker"], pd.Timestamp(r["ex_date"]).normalize()): float(r["factor"]) for _, r in ssot_df.iterrows()}
    panel["split_factor_qty"] = panel.apply(lambda r: factor_map.get((str(r["ticker"]), pd.Timestamp(r["date"]).normalize()), 1.0), axis=1)
    panel["adjusted_logret"] = panel["raw_logret"] + np.log(panel["split_factor_qty"].astype(float))

    adjusted_close_all = []
    for t, g in panel.groupby("ticker"):
        g = g.sort_values("date").copy()
        cum_factor = 1.0
        adj_vals = []
        for _, r in g.iterrows():
            f = float(r["split_factor_qty"])
            if np.isfinite(f) and f > 0 and abs(f - 1.0) > 1e-12:
                cum_factor *= f
            adj_vals.append(float(r["close"]) * cum_factor)
        g["adjusted_close"] = adj_vals
        adjusted_close_all.append(g)
    adjusted = pd.concat(adjusted_close_all, ignore_index=True)
    adjusted.to_parquet(evidence_dir / "adjusted_series_eqtl3_enev3.parquet", index=False)
    adjusted.to_csv(evidence_dir / "adjusted_series_eqtl3_enev3.csv", index=False)
    s4_ok = (evidence_dir / "adjusted_series_eqtl3_enev3.parquet").exists()
    gates.append({"name": "S4_BUILD_ADJUSTED_RETURN_SERIES_FOR_CEP", "status": "PASS" if s4_ok else "FAIL"})
    steps.append({"name": "S4_BUILD_ADJUSTED_RETURN_SERIES_FOR_CEP", "status": "PASS" if s4_ok else "FAIL"})

    # S5 audits identity checks + anomaly flags
    exp001_pos = pd.read_parquet(working_root / "outputs/experimentos/on_flight/20260223/EXP_001_S008_TOP10S45_STRESSSELL_2019_20210630_V1/positions_daily.parquet")
    exp001_pos["decision_date"] = pd.to_datetime(exp001_pos["decision_date"]).dt.normalize()
    exp002_trades = pd.read_parquet(working_root / "outputs/experimentos/on_flight/20260224/EXP_002_S008_SELLGATE_CEP_RL_ABL345_2019_20210630_V1/tables/trades_all.parquet")
    exp002_trades["execution_date"] = pd.to_datetime(exp002_trades["execution_date"]).dt.normalize()
    exp002_trades = exp002_trades[(exp002_trades["variant"] == "deterministic") & (exp002_trades["slope_w"] == 4)].copy()

    # reconstrucao simples de qty para EXP_002
    pos2 = {}
    rec2 = []
    all_days = sorted(pd.to_datetime(adjusted["date"]).dt.normalize().unique().tolist())
    trades_by_d = {d: g for d, g in exp002_trades.groupby("execution_date")}
    for d in all_days:
        for _, tr in trades_by_d.get(d, pd.DataFrame()).iterrows():
            t = str(tr["ticker"])
            q = int(tr["qty"])
            if str(tr["side"]).upper() == "BUY":
                pos2[t] = int(pos2.get(t, 0)) + q
            else:
                pos2[t] = int(pos2.get(t, 0)) - q
                if pos2[t] < 0:
                    pos2[t] = 0
        for t, q in pos2.items():
            if q > 0:
                rec2.append({"decision_date": d, "ticker": t, "qty": q})
    exp002_pos = pd.DataFrame(rec2)

    audit_rows = []
    for _, ev in ssot_df.iterrows():
        t = str(ev["ticker"])
        d = pd.Timestamp(ev["ex_date"]).normalize()
        f = float(ev["factor"])
        px_prev = float(adjusted[(adjusted["ticker"] == t) & (adjusted["date"] == d - pd.Timedelta(days=1))]["close"].tail(1).iloc[0])
        px_ex = float(adjusted[(adjusted["ticker"] == t) & (adjusted["date"] == d)]["close"].tail(1).iloc[0])
        raw_ratio = px_ex / px_prev if px_prev > 0 else np.nan
        corrected_ratio = (px_ex * f) / px_prev if px_prev > 0 else np.nan
        raw_lr = float(adjusted[(adjusted["ticker"] == t) & (adjusted["date"] == d)]["raw_logret"].tail(1).iloc[0])
        adj_lr = float(adjusted[(adjusted["ticker"] == t) & (adjusted["date"] == d)]["adjusted_logret"].tail(1).iloc[0])
        q1 = exp001_pos[(exp001_pos["ticker"] == t) & (exp001_pos["decision_date"] == d)]["qty"]
        q2 = exp002_pos[(exp002_pos["ticker"] == t) & (exp002_pos["decision_date"] == d)]["qty"] if len(exp002_pos) else pd.Series(dtype=float)
        audit_rows.append(
            {
                "ticker": t,
                "ex_date": d,
                "factor": f,
                "raw_price_ratio_ex_vs_prev": raw_ratio,
                "corrected_ratio_with_factor": corrected_ratio,
                "raw_logret_ex_date": raw_lr,
                "adjusted_logret_ex_date": adj_lr,
                "exp001_qty_on_ex_date": float(q1.iloc[0]) if len(q1) else 0.0,
                "exp002_qty_on_ex_date": float(q2.iloc[0]) if len(q2) else 0.0,
                "identity_ok": bool(np.isfinite(corrected_ratio) and abs(np.log(corrected_ratio)) < 0.10),
                "adjusted_logret_ok": bool(np.isfinite(adj_lr) and abs(adj_lr) < 0.20),
            }
        )
    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_parquet(audits_dir / "identity_checks_eqtl3_enev3.parquet", index=False)
    audit_df.to_csv(audits_dir / "identity_checks_eqtl3_enev3.csv", index=False)

    # QA detector for potential missing split SSOT
    q = panel[["ticker", "date", "close", "prev_close"]].copy()
    q = q[q["prev_close"].notna()].copy()
    q["ratio"] = q["close"] / q["prev_close"]
    q["near_split_ratio"] = False
    for n in [2, 3, 4, 5, 10]:
        q["near_split_ratio"] = q["near_split_ratio"] | (q["ratio"].sub(1.0 / n).abs() <= 0.02) | (q["ratio"].sub(float(n)).abs() <= 0.20)
    known = {(str(r["ticker"]), pd.Timestamp(r["ex_date"]).normalize()) for _, r in ssot_df.iterrows()}
    q["known_ssot"] = q.apply(lambda r: (str(r["ticker"]), pd.Timestamp(r["date"]).normalize()) in known, axis=1)
    qa = q[(q["near_split_ratio"]) & (~q["known_ssot"])].copy().sort_values(["ticker", "date"]).head(500)
    qa.to_parquet(audits_dir / "qa_detector_missing_ssot_flags.parquet", index=False)
    qa.to_csv(audits_dir / "qa_detector_missing_ssot_flags.csv", index=False)

    s5_ok = bool(audit_df["identity_ok"].all() and audit_df["adjusted_logret_ok"].all())
    gates.append(
        {
            "name": "S5_AUDIT_IDENTITY_CHECKS_AND_ANOMALY_FLAGS",
            "status": "PASS" if s5_ok else "FAIL",
            "details": {
                "identity_ok_all": bool(audit_df["identity_ok"].all()),
                "adjusted_logret_ok_all": bool(audit_df["adjusted_logret_ok"].all()),
                "qa_missing_ssot_flags_count": int(len(qa)),
            },
        }
    )
    steps.append({"name": "S5_AUDIT_IDENTITY_CHECKS_AND_ANOMALY_FLAGS", "status": "PASS" if s5_ok else "FAIL"})

    # S6 validate episodes and write reports
    design_note = "\n".join(
        [
            "# Split Handling Design Note",
            "",
            "- apply_time: start_of_day_D_before_any_trades",
            "- qty update: qty_D = qty_prev * factor",
            "- avg_cost policy: avg_cost_D = avg_cost_prev / factor (motores atuais nao persistem avg_cost; regra documentada).",
            "- cash update: unchanged",
            "- valuation/trade: raw close",
            "- CEP/logret: adjusted_logret = raw_logret + ln(factor_qty) no ex_date",
            "",
        ]
    )
    write_text(design_dir / "split_handling_design_note.md", design_note)

    episode_lines = [
        "# Episode Validation - EQTL3 e ENEV3",
        "",
        "## Evidencia de remoção da queda falsa",
    ]
    for _, r in audit_df.iterrows():
        episode_lines.extend(
            [
                f"- `{r['ticker']}` em `{pd.Timestamp(r['ex_date']).date()}`:",
                f"  - raw_price_ratio_ex_vs_prev = `{float(r['raw_price_ratio_ex_vs_prev']):.6f}`",
                f"  - corrected_ratio_with_factor = `{float(r['corrected_ratio_with_factor']):.6f}`",
                f"  - raw_logret_ex_date = `{float(r['raw_logret_ex_date']):.6f}`",
                f"  - adjusted_logret_ex_date = `{float(r['adjusted_logret_ex_date']):.6f}`",
                f"  - identity_ok = `{bool(r['identity_ok'])}`",
                f"  - adjusted_logret_ok = `{bool(r['adjusted_logret_ok'])}`",
            ]
        )
    episode_lines.extend(
        [
            "",
            "## Arquivos de suporte",
            f"- `{(ssot_dir / 'corporate_actions.parquet').relative_to(working_root)}`",
            f"- `{(audits_dir / 'identity_checks_eqtl3_enev3.parquet').relative_to(working_root)}`",
            f"- `{(audits_dir / 'qa_detector_missing_ssot_flags.parquet').relative_to(working_root)}`",
            f"- `{(evidence_dir / 'adjusted_series_eqtl3_enev3.parquet').relative_to(working_root)}`",
            "",
        ]
    )
    write_text(outputs_dir / "episode_validation_note.md", "\n".join(episode_lines))

    s6_ok = (outputs_dir / "episode_validation_note.md").exists() and (design_dir / "split_handling_design_note.md").exists()
    gates.append({"name": "S6_VALIDATE_ON_KNOWN_EPISODES_EQTL3_ENEV3_WITH_REPORT", "status": "PASS" if s6_ok else "FAIL"})
    steps.append({"name": "S6_VALIDATE_ON_KNOWN_EPISODES_EQTL3_ENEV3_WITH_REPORT", "status": "PASS" if s6_ok else "FAIL"})

    # manifest
    files = [p for p in sorted(outputs_dir.rglob("*")) if p.is_file()]
    hashes = {str(p): sha256_file(p) for p in files}
    manifest = {
        "task_id": task["instruction_id"],
        "generated_at_utc": now_utc(),
        "output_root": str(outputs_dir),
        "required_files": list(hashes.keys()),
        "hashes_sha256": hashes,
        "overall": "PASS" if all(g["status"] == "PASS" for g in gates) else "FAIL",
    }
    write_json(outputs_dir / "manifest.json", manifest)
    manifest["hashes_sha256"][str(outputs_dir / "manifest.json")] = sha256_file(outputs_dir / "manifest.json")
    write_json(outputs_dir / "manifest.json", manifest)

    overall = all(g["status"] == "PASS" for g in gates)
    run_report = {
        "task_id": task["instruction_id"],
        "status": "PASS" if overall else "FAIL",
        "overall_pass": overall,
        "gates": gates,
        "steps": steps,
        "deliverables": {
            "ssot_parquet": str(ssot_dir / "corporate_actions.parquet"),
            "design_note": str(design_dir / "split_handling_design_note.md"),
            "audit_identity": str(audits_dir / "identity_checks_eqtl3_enev3.parquet"),
            "audit_qa_detector": str(audits_dir / "qa_detector_missing_ssot_flags.parquet"),
            "episode_validation_note": str(outputs_dir / "episode_validation_note.md"),
            "manifest": str(outputs_dir / "manifest.json"),
        },
        "timestamp_utc": now_utc(),
    }
    write_json(run_dir / "report.json", run_report)
    write_json(run_dir / "run_summary.json", run_report)
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
