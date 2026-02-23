from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


OFFICIAL_PYTHON = "/home/wilson/PortfolioZero/.venv/bin/python"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def ensure_official_python() -> None:
    current = str(Path(__import__("sys").executable))
    if current != OFFICIAL_PYTHON:
        raise RuntimeError(f"Interpreter invalido: {current}. Use exclusivamente {OFFICIAL_PYTHON}.")


def run_cmd(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def derive_owner_expected_448(df_daily: pd.DataFrame, df_cov: pd.DataFrame, sig_col: str, k: int, n: int) -> list[str]:
    """
    Reproduz a lista validada no chat ('TICKERS (interseção) — ordenados:'):
    interseção entre baseline_ok e janela contínua de W=K+N-1 iniciando em 2025-10-30.
    """
    w = k + n - 1
    start = pd.Timestamp("2025-10-30")

    baseline_ok_set = set(df_cov.loc[df_cov["baseline_ok"] == True, "ticker"].astype(str).unique().tolist())  # noqa: E712

    work = df_daily[["date", "ticker", sig_col]].copy()
    work["date"] = pd.to_datetime(work["date"])
    work["ticker"] = work["ticker"].astype(str)
    work = work.sort_values(["ticker", "date"])
    work["valid"] = work[sig_col].notna().astype(int)
    work["win_valid_sum"] = work.groupby("ticker")["valid"].rolling(window=w, min_periods=w).sum().reset_index(level=0, drop=True)
    work["start_date"] = work.groupby("ticker")["date"].shift(w - 1)

    inter = work[(work["start_date"] == start) & (work["win_valid_sum"] == w)]["ticker"].drop_duplicates()
    inter_set = set(inter.astype(str).tolist())
    out = sorted(baseline_ok_set.intersection(inter_set))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    ensure_official_python()

    task_spec_path = Path(args.task_spec).resolve()
    task_spec = json.loads(task_spec_path.read_text(encoding="utf-8"))

    repo_root = Path(task_spec["repo_root"]).resolve()
    attempt2_root = Path(task_spec["execution_root"]).resolve()
    out_root = Path(task_spec["outputs"]["output_root"]).resolve()
    evidence_dir = out_root / "evidence"
    visual_dir = out_root / "visual"
    out_root.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    visual_dir.mkdir(parents=True, exist_ok=True)

    base_daily_path = Path(task_spec["inputs"]["base_operacional_daily_parquet"]).resolve()
    base_cov_path = Path(task_spec["inputs"]["base_operacional_coverage_parquet"]).resolve()
    checklist_path = Path(task_spec["inputs"]["owner_visual_checklist_md"]).resolve()
    expected_file = Path(task_spec["inputs"]["expected_tickers_file_to_create"]).resolve()

    anchor_date = pd.Timestamp(task_spec["inputs"]["anchor_date"])
    sig_col = task_spec["inputs"]["sig_col"]
    k = int(task_spec["inputs"]["params"]["K"])
    n = int(task_spec["inputs"]["params"]["N"])
    expected_baseline_ok_count = int(task_spec["inputs"]["params"]["expected_baseline_ok_count"])
    expected_selected_count = int(task_spec["inputs"]["params"]["expected_selected_count"])
    expected_breakdown = {k_: int(v) for k_, v in task_spec["inputs"]["params"]["expected_breakdown_selected"].items()}

    gates: dict[str, dict[str, Any]] = {}
    generated_at = now_utc_iso()

    # S1_GATE_ALLOWLIST
    snapshot_path = attempt2_root / "ssot_snapshot"
    snapshot_status = run_cmd(["git", "status", "--short", "--", str(snapshot_path)], repo_root).stdout.strip()
    s1_ok = str(base_daily_path).startswith(str(attempt2_root)) and str(base_cov_path).startswith(str(attempt2_root))
    s1_ok = s1_ok and (snapshot_status == "")
    write_text(evidence_dir / "allowlist_check.txt", f"inputs_within_attempt2={s1_ok}\nsnapshot_status={snapshot_status or 'NO_CHANGES'}\n")
    gates["S1_GATE_ALLOWLIST"] = {"pass": s1_ok, "evidence": str(evidence_dir / "allowlist_check.txt")}

    # Load base data
    df_daily = pd.read_parquet(base_daily_path)
    df_cov = pd.read_parquet(base_cov_path)
    df_daily["date"] = pd.to_datetime(df_daily["date"])
    df_daily["ticker"] = df_daily["ticker"].astype(str)
    df_cov["ticker"] = df_cov["ticker"].astype(str)

    if sig_col not in df_daily.columns:
        raise RuntimeError(f"sig_col ausente em base_operacional_daily: {sig_col}")

    # Step 0: create expected tickers file using ordered deterministic list validated in chat
    expected_tickers = derive_owner_expected_448(df_daily, df_cov, sig_col=sig_col, k=k, n=n)
    expected_file.parent.mkdir(parents=True, exist_ok=True)
    write_text(expected_file, "\n".join(expected_tickers) + "\n")

    # S2_CHECK_COMPILE
    compile_target = Path(__file__).resolve()
    pyc = run_cmd([OFFICIAL_PYTHON, "-m", "py_compile", str(compile_target)], repo_root)
    s2_ok = pyc.returncode == 0
    write_text(evidence_dir / "compile_check.txt", f"returncode={pyc.returncode}\nstdout={pyc.stdout}\nstderr={pyc.stderr}\n")
    gates["S2_CHECK_COMPILE"] = {"pass": s2_ok, "evidence": str(evidence_dir / "compile_check.txt")}

    # S3_RUN (core processing)
    try:
        # Step 1 baseline set A
        df_cov["baseline_ok"] = df_cov["baseline_ok"].fillna(False).astype(bool)
        set_a_df = df_cov[df_cov["baseline_ok"]].copy()
        baseline_ok_count = int(len(set_a_df))
        baseline_breakdown = (
            set_a_df.groupby("asset_class", dropna=False)["ticker"]
            .count()
            .rename("count")
            .reset_index()
            .sort_values("asset_class")
        )
        baseline_breakdown.to_csv(evidence_dir / "baseline_ok_557_breakdown.csv", index=False)

        if baseline_ok_count != expected_baseline_ok_count:
            raise RuntimeError(f"baseline_ok_count inesperado: {baseline_ok_count} != {expected_baseline_ok_count}")

        # Step 2 dataset_end_date
        dataset_end_date = pd.Timestamp(df_daily["date"].max())

        # Step 3 canonical calendar
        has_bvsp = (df_daily["ticker"] == "^BVSP").any()
        if has_bvsp:
            cal = (
                df_daily[(df_daily["ticker"] == "^BVSP") & (df_daily["date"] >= anchor_date) & (df_daily["date"] <= dataset_end_date)]["date"]
                .drop_duplicates()
                .sort_values()
            )
            calendar_source = "^BVSP"
        else:
            cal = (
                df_daily[(df_daily["date"] >= anchor_date) & (df_daily["date"] <= dataset_end_date)]["date"]
                .drop_duplicates()
                .sort_values()
            )
            calendar_source = "GLOBAL_UNIQUE_DATES_FROM_PANEL"

        cal_list = [pd.Timestamp(d).normalize() for d in pd.to_datetime(cal).tolist()]

        write_text(evidence_dir / "calendar_source.txt", f"{calendar_source}\n")
        write_text(evidence_dir / "canonical_calendar_dates_list.txt", "\n".join([d.strftime("%Y-%m-%d") for d in cal_list]) + "\n")
        pd.DataFrame(
            [
                {
                    "calendar_source": calendar_source,
                    "min_date": str(min(cal_list)) if cal_list else None,
                    "max_date": str(max(cal_list)) if cal_list else None,
                    "count": len(cal_list),
                }
            ]
        ).to_csv(evidence_dir / "canonical_calendar_summary.csv", index=False)

        # Step 4 continuity stats for all tickers
        per_rows: list[dict[str, Any]] = []
        missing_samples: list[str] = []
        for ticker, g in df_daily.groupby("ticker", sort=True):
            g = g.sort_values("date")
            g_valid = g[g[sig_col].notna()]
            if len(g_valid) > 0:
                first_date = pd.Timestamp(g_valid["date"].min()).normalize()
                last_date = pd.Timestamp(g_valid["date"].max()).normalize()
            else:
                first_date = pd.Timestamp(g["date"].min()).normalize()
                last_date = pd.Timestamp(g["date"].max()).normalize()
            eff_start = max(anchor_date, first_date)
            cal_t = [d for d in cal_list if d >= eff_start]
            req_count = len(cal_t)

            present_dates = set(pd.to_datetime(g.loc[g[sig_col].notna(), "date"]).dt.normalize().tolist())
            present_count = sum(1 for d in cal_t if d in present_dates)
            missing_count = req_count - present_count

            continuity_ok = missing_count == 0
            if (not continuity_ok) and len(missing_samples) < 25:
                missing_dates = [d.strftime("%Y-%m-%d") for d in cal_t if d not in present_dates][:10]
                missing_samples.append(f"{ticker}: missing_count={missing_count}; sample={missing_dates}")

            row_cov = df_cov[df_cov["ticker"] == ticker].head(1)
            asset_class = row_cov["asset_class"].iloc[0] if len(row_cov) else "UNKNOWN"
            baseline_ok = bool(row_cov["baseline_ok"].iloc[0]) if len(row_cov) else False

            per_rows.append(
                {
                    "ticker": ticker,
                    "asset_class": asset_class,
                    "first_date": first_date.strftime("%Y-%m-%d"),
                    "last_date": last_date.strftime("%Y-%m-%d"),
                    "effective_start_date": eff_start.strftime("%Y-%m-%d"),
                    "dataset_end_date": dataset_end_date.strftime("%Y-%m-%d"),
                    "required_days_count": req_count,
                    "present_days_count": present_count,
                    "missing_days_count": missing_count,
                    "baseline_ok_k60_n3": baseline_ok,
                    "continuity_ok": continuity_ok,
                }
            )

        per_df = pd.DataFrame(per_rows).sort_values("ticker").reset_index(drop=True)
        per_df.to_csv(evidence_dir / "per_ticker_continuity_stats.csv", index=False)
        write_text(evidence_dir / "missing_dates_samples.txt", "\n".join(missing_samples) + ("\n" if missing_samples else "NO_MISSING_SAMPLES\n"))

        # Step 5 selected S
        selected = per_df[(per_df["baseline_ok_k60_n3"] == True) & (per_df["missing_days_count"] == 0)].copy()  # noqa: E712
        selected = selected.sort_values("ticker").reset_index(drop=True)
        selected_count = int(len(selected))

        selected_breakdown = (
            selected.groupby("asset_class", dropna=False)["ticker"]
            .count()
            .rename("count")
            .reset_index()
            .sort_values("asset_class")
        )
        selected_breakdown.to_csv(evidence_dir / "selected_breakdown.csv", index=False)

        selected_breakdown_dict = {r["asset_class"]: int(r["count"]) for _, r in selected_breakdown.iterrows()}
        if selected_count != expected_selected_count:
            raise RuntimeError(f"selected_count inesperado: {selected_count} != {expected_selected_count}")
        if selected_breakdown_dict != expected_breakdown:
            raise RuntimeError(f"selected_breakdown inesperado: {selected_breakdown_dict} != {expected_breakdown}")

        # Step 6 compare selected vs expected list
        expected_lines = [ln.strip() for ln in expected_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        selected_lines = selected["ticker"].astype(str).tolist()
        selected_set = set(selected_lines)
        expected_set = set(expected_lines)

        only_selected = sorted(selected_set - expected_set)
        only_expected = sorted(expected_set - selected_set)
        exact_equal = (selected_lines == expected_lines) and (len(only_selected) == 0) and (len(only_expected) == 0)

        diff_lines = ["PASS" if exact_equal else "FAIL"]
        if not exact_equal:
            diff_lines.append("--- only_in_selected ---")
            diff_lines.extend(only_selected)
            diff_lines.append("--- only_in_expected ---")
            diff_lines.extend(only_expected)
            diff_lines.append("--- order_mismatch_check ---")
            diff_lines.append("selected_and_expected_have_different_order")
        write_text(evidence_dir / "selected_vs_expected_diff.txt", "\n".join(diff_lines) + "\n")
        write_text(evidence_dir / "selected_tickers_sorted.txt", "\n".join(selected_lines) + "\n")
        if not exact_equal:
            raise RuntimeError("selected_set difere de expected list")

        # Step 7 build SSOT parquet + md
        source_paths = json.dumps(
            [
                str(base_daily_path),
                str(base_cov_path),
                str(expected_file),
            ],
            ensure_ascii=False,
        )
        ssot_df = selected[
            [
                "ticker",
                "asset_class",
                "first_date",
                "last_date",
                "effective_start_date",
                "dataset_end_date",
                "required_days_count",
                "present_days_count",
                "missing_days_count",
                "baseline_ok_k60_n3",
            ]
        ].copy()
        ssot_df["sig_col"] = sig_col
        ssot_df["calendar_source"] = calendar_source
        ssot_df["selection_version"] = "v1"
        ssot_df["created_at_utc"] = generated_at
        ssot_df["source_paths"] = source_paths

        ssot_parquet_path = out_root / "ssot_approved_tickers.parquet"
        ssot_df.to_parquet(ssot_parquet_path, index=False)

        md_lines = [
            "# SSOT Approved Tickers - Continuous from 2018-01-01 or IPO (K=60,N=3)",
            "",
            f"- generated_at_utc: `{generated_at}`",
            f"- selected_count: `{selected_count}`",
            f"- sig_col: `{sig_col}`",
            f"- calendar_source: `{calendar_source}`",
            f"- dataset_end_date: `{dataset_end_date.strftime('%Y-%m-%d')}`",
            "",
            "## Breakdown",
        ]
        for _, r in selected_breakdown.iterrows():
            md_lines.append(f"- {r['asset_class']}: {int(r['count'])}")
        md_lines.extend(["", "## Tickers (sorted)"])
        md_lines.extend([f"- {t}" for t in selected_lines])
        write_text(out_root / "ssot_approved_tickers.md", "\n".join(md_lines) + "\n")

        # Step 8 visual update (ACTIVE chain with new S005 only)
        checklist_content = """# Checklist — Estado Confirmado (Owner) — CEP_BUNDLE_CORE (Tentativa 2)

last_updated: 2026-02-22
scope: Tentativa 2 (work/ e outputs/ apenas; ssot_snapshot/ read-only)

Regra: este checklist mostra apenas o que existe como correto (confirmado e materializado). Nao listar pendencias.

```mermaid
flowchart TB
  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]
  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]
  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]
  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]
  S005["S005 SSOT tickers aprovados (contínuos desde 2018/IPO)"]
  S001 --> S002 --> S003 --> S004 --> S005
```
"""
        write_text(checklist_path, checklist_content)

        decisions_flow = """flowchart TB
  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]
  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]
  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]
  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]
  S005["S005 SSOT tickers aprovados (contínuos desde 2018/IPO)"]

  S001 --> S002 --> S003 --> S004 --> S005
"""
        write_text(visual_dir / "decisions_flow.mmd", decisions_flow)

        # Step 9 deprecations note
        deprec_lines = [
            "# Deprecations Note (Active Chain)",
            "",
            "Itens preservados para auditoria/referência e removidos da cadeia ACTIVE:",
            "",
            "- S005 anterior: base_operacional_v1",
            "- S006 anterior: burners_ranking_spc_v2",
            "",
            "Status:",
            "- Não deletados.",
            "- Mantidos em outputs para rastreabilidade histórica.",
        ]
        write_text(evidence_dir / "deprecations_note.md", "\n".join(deprec_lines) + "\n")

        s3_ok = True
    except Exception as exc:
        write_text(evidence_dir / "run_exception.txt", f"{type(exc).__name__}: {exc}\n")
        s3_ok = False

    gates["S3_RUN"] = {"pass": s3_ok, "evidence": str(evidence_dir / ("run_exception.txt" if not s3_ok else "selected_tickers_sorted.txt"))}

    # S4_VERIFY_SELECTION
    s4_conditions = []
    s4_ok = False
    if s3_ok:
        selected_breakdown_csv = pd.read_csv(evidence_dir / "selected_breakdown.csv")
        selected_breakdown_dict = {r["asset_class"]: int(r["count"]) for _, r in selected_breakdown_csv.iterrows()}
        selected_count = int(pd.read_parquet(out_root / "ssot_approved_tickers.parquet").shape[0])
        baseline_count = int(pd.read_csv(evidence_dir / "baseline_ok_557_breakdown.csv")["count"].sum())
        diff_pass = (evidence_dir / "selected_vs_expected_diff.txt").read_text(encoding="utf-8").splitlines()[0].strip() == "PASS"
        per_df = pd.read_csv(evidence_dir / "per_ticker_continuity_stats.csv")
        selected_set = set(pd.read_parquet(out_root / "ssot_approved_tickers.parquet")["ticker"].astype(str).tolist())
        selected_missing_nonzero = int(per_df[per_df["ticker"].astype(str).isin(selected_set)]["missing_days_count"].sum())

        s4_conditions = [
            baseline_count == expected_baseline_ok_count,
            selected_count == expected_selected_count,
            selected_breakdown_dict == expected_breakdown,
            diff_pass,
            (evidence_dir / "calendar_source.txt").exists() and (evidence_dir / "canonical_calendar_summary.csv").exists(),
            selected_missing_nonzero == 0,
        ]
        s4_ok = all(s4_conditions)
    gates["S4_VERIFY_SELECTION"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5_VERIFY_OUTPUTS_AND_HASHES
    required_rel = task_spec["outputs"]["required_files"]
    required_abs = [out_root / rel for rel in required_rel]
    all_exists = all(p.exists() for p in required_abs)

    manifest_path = out_root / "manifest.json"
    hashes = {}
    for p in required_abs:
        if p.exists() and p.is_file():
            hashes[str(p)] = sha256_file(p)
    manifest_payload = {
        "task_id": task_spec["task_id"],
        "generated_at_utc": generated_at,
        "task_spec_path": str(task_spec_path),
        "output_root": str(out_root),
        "gates": gates,
        "required_files": [str(p) for p in required_abs],
        "hashes_sha256": hashes,
    }
    write_json(manifest_path, manifest_payload)

    s5_ok = all_exists and len(hashes) >= len([p for p in required_abs if p.is_file()])
    gates["S5_VERIFY_OUTPUTS_AND_HASHES"] = {"pass": s5_ok, "required_exists": all_exists}

    # report
    overall = all(v.get("pass") is True for v in gates.values())
    report_lines = [
        "# Report - SSOT Approved Tickers Continuous from 2018-01-01 or IPO (K=60,N=3) V1",
        "",
        f"- task_id: `{task_spec['task_id']}`",
        f"- generated_at_utc: `{generated_at}`",
        f"- overall: `{'PASS' if overall else 'FAIL'}`",
        "",
        "## Gates",
    ]
    for name in ["S1_GATE_ALLOWLIST", "S2_CHECK_COMPILE", "S3_RUN", "S4_VERIFY_SELECTION", "S5_VERIFY_OUTPUTS_AND_HASHES"]:
        report_lines.append(f"- {name}: `{'PASS' if gates.get(name, {}).get('pass') else 'FAIL'}`")

    report_lines.extend(
        [
            "",
            "## Outputs principais",
            f"- `{out_root / 'ssot_approved_tickers.parquet'}`",
            f"- `{out_root / 'ssot_approved_tickers.md'}`",
            f"- `{out_root / 'manifest.json'}`",
            f"- `{checklist_path}`",
        ]
    )
    write_text(out_root / "report.md", "\n".join(report_lines) + "\n")

    # rewrite manifest with final gates/report hash
    manifest_payload["gates"] = gates
    manifest_payload["overall"] = "PASS" if overall else "FAIL"
    if (out_root / "report.md").exists():
        manifest_payload["hashes_sha256"][str(out_root / "report.md")] = sha256_file(out_root / "report.md")
    write_json(manifest_path, manifest_payload)

    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

