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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    ensure_official_python()

    spec_path = Path(args.task_spec).resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))

    repo_root = Path(spec["repo_root"]).resolve()
    attempt2_root = Path(spec["execution_root"]).resolve()
    out_root = Path(spec["outputs"]["output_root"]).resolve()
    evidence_dir = out_root / "evidence"
    visual_dir = out_root / "visual"
    out_root.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    visual_dir.mkdir(parents=True, exist_ok=True)

    base_daily_path = Path(spec["inputs"]["base_operacional_daily_parquet"]).resolve()
    base_cov_path = Path(spec["inputs"]["base_operacional_coverage_parquet"]).resolve()
    checklist_path = Path(spec["inputs"]["owner_visual_checklist_md"]).resolve()
    expected_file = Path(spec["inputs"]["expected_tickers_file"]).resolve()

    window_start = pd.Timestamp(spec["inputs"]["selection_window"]["start_date"]).normalize()
    sig_col = spec["inputs"]["selection_window"]["sig_col"]
    expected_w = int(spec["inputs"]["selection_window"]["expected_W"])
    history_anchor = pd.Timestamp(spec["inputs"]["history_audit"]["anchor_date"]).normalize()

    k = int(spec["inputs"]["params"]["K"])
    n = int(spec["inputs"]["params"]["N"])
    expected_baseline_ok_count = int(spec["inputs"]["params"]["expected_baseline_ok_count"])
    expected_selected_count = int(spec["inputs"]["params"]["expected_selected_count"])
    expected_breakdown = {kk: int(vv) for kk, vv in spec["inputs"]["params"]["expected_breakdown_selected"].items()}

    generated_at = now_utc_iso()
    gates: dict[str, dict[str, Any]] = {}

    # S1 gate allowlist + snapshot unchanged
    snapshot = attempt2_root / "ssot_snapshot"
    snapshot_status = run_cmd(["git", "status", "--short", "--", str(snapshot)], repo_root).stdout.strip()
    allow_ok = str(base_daily_path).startswith(str(attempt2_root)) and str(base_cov_path).startswith(str(attempt2_root))
    allow_ok = allow_ok and (snapshot_status == "")
    write_text(evidence_dir / "allowlist_check.txt", f"allow_ok={allow_ok}\nsnapshot_status={snapshot_status or 'NO_CHANGES'}\n")
    gates["S1_GATE_ALLOWLIST"] = {"pass": allow_ok, "evidence": str(evidence_dir / "allowlist_check.txt")}

    # load data
    df_daily = pd.read_parquet(base_daily_path)
    df_cov = pd.read_parquet(base_cov_path)
    df_daily["date"] = pd.to_datetime(df_daily["date"]).dt.normalize()
    df_daily["ticker"] = df_daily["ticker"].astype(str)
    df_cov["ticker"] = df_cov["ticker"].astype(str)
    if sig_col not in df_daily.columns:
        raise RuntimeError(f"sig_col não encontrado no painel: {sig_col}")
    if "baseline_ok" not in df_cov.columns:
        raise RuntimeError("baseline_ok não encontrado em base_operacional_coverage")

    # Step 0 expected list file (reuse prior generated expected list)
    fallback_expected = attempt2_root / "work/reference/universe_expected_448_from_owner_CONTINUOUS_2018_OR_IPO.txt"
    if (not expected_file.exists()) and fallback_expected.exists():
        expected_file.parent.mkdir(parents=True, exist_ok=True)
        expected_file.write_text(fallback_expected.read_text(encoding="utf-8"), encoding="utf-8")
    if not expected_file.exists():
        raise RuntimeError(f"expected_tickers_file ausente: {expected_file}")

    # S2 compile
    compile_res = run_cmd([OFFICIAL_PYTHON, "-m", "py_compile", str(Path(__file__).resolve())], repo_root)
    compile_ok = compile_res.returncode == 0
    write_text(evidence_dir / "compile_check.txt", f"returncode={compile_res.returncode}\nstdout={compile_res.stdout}\nstderr={compile_res.stderr}\n")
    gates["S2_CHECK_COMPILE"] = {"pass": compile_ok, "evidence": str(evidence_dir / "compile_check.txt")}

    try:
        # Step 1 baseline A
        df_cov["baseline_ok"] = df_cov["baseline_ok"].fillna(False).astype(bool)
        baseline_df = df_cov[df_cov["baseline_ok"]].copy()
        baseline_ok_count = int(len(baseline_df))
        if baseline_ok_count != expected_baseline_ok_count:
            raise RuntimeError(f"baseline_ok_count inesperado: {baseline_ok_count} != {expected_baseline_ok_count}")

        baseline_breakdown = baseline_df.groupby("asset_class", dropna=False)["ticker"].count().rename("count").reset_index().sort_values("asset_class")
        baseline_breakdown.to_csv(evidence_dir / "baseline_ok_557_breakdown.csv", index=False)
        set_a = set(baseline_df["ticker"].astype(str).tolist())

        # Step 2 CAL_win global unique dates >= window_start (janela fixa W=62)
        dataset_end = pd.Timestamp(df_daily["date"].max()).normalize()
        cal_from_start = sorted(df_daily.loc[df_daily["date"] >= window_start, "date"].drop_duplicates().tolist())
        cal_win = cal_from_start[:expected_w]
        window_w = len(cal_win)
        if window_w != expected_w:
            raise RuntimeError(f"window_W inesperado: {window_w} != {expected_w}")
        cal_win_set = set(cal_win)

        write_text(evidence_dir / "calendar_source.txt", "GLOBAL_UNIQUE_DATES_FROM_PANEL\n")
        write_text(evidence_dir / "window_calendar_dates_list.txt", "\n".join([d.strftime("%Y-%m-%d") for d in cal_win]) + "\n")
        pd.DataFrame(
            [
                {
                    "calendar_source": "GLOBAL_UNIQUE_DATES_FROM_PANEL",
                    "window_start": window_start.strftime("%Y-%m-%d"),
                    "window_end": dataset_end.strftime("%Y-%m-%d"),
                    "window_W": window_w,
                }
            ]
        ).to_csv(evidence_dir / "window_calendar_summary.csv", index=False)

        # first_date / last_date per ticker (row existence)
        ticker_bounds = (
            df_daily.groupby("ticker", dropna=False)["date"]
            .agg(first_date="min", last_date="max")
            .reset_index()
        )
        ticker_bounds["first_date"] = pd.to_datetime(ticker_bounds["first_date"]).dt.normalize()
        ticker_bounds["last_date"] = pd.to_datetime(ticker_bounds["last_date"]).dt.normalize()
        first_date_map = dict(zip(ticker_bounds["ticker"], ticker_bounds["first_date"]))
        last_date_map = dict(zip(ticker_bounds["ticker"], ticker_bounds["last_date"]))

        # Step 3 B_win continuity by sig_col non-null over full CAL_win
        win_present = (
            df_daily[(df_daily["date"].isin(cal_win_set)) & (df_daily[sig_col].notna())]
            .groupby("ticker", dropna=False)["date"]
            .nunique()
            .rename("present_days_count")
            .reset_index()
        )
        win_present_map = dict(zip(win_present["ticker"], win_present["present_days_count"]))
        set_bwin = set([t for t, cnt in win_present_map.items() if int(cnt) == window_w])

        # Step 4 existed_before
        set_existed = set([t for t, fd in first_date_map.items() if pd.Timestamp(fd) < window_start])

        # Step 5 selected S
        selected_set = set_a.intersection(set_bwin).intersection(set_existed)
        selected_sorted = sorted(selected_set)
        selected_count = len(selected_sorted)
        if selected_count != expected_selected_count:
            raise RuntimeError(f"selected_count inesperado: {selected_count} != {expected_selected_count}")

        selected_df = baseline_df[baseline_df["ticker"].isin(selected_sorted)][["ticker", "asset_class", "baseline_ok"]].copy()
        selected_df = selected_df.drop_duplicates(subset=["ticker"]).sort_values("ticker")
        selected_breakdown = selected_df.groupby("asset_class", dropna=False)["ticker"].count().rename("count").reset_index().sort_values("asset_class")
        selected_breakdown.to_csv(evidence_dir / "selected_448_breakdown.csv", index=False)
        write_text(evidence_dir / "selected_448_tickers_sorted.txt", "\n".join(selected_sorted) + "\n")
        selected_breakdown_dict = {r["asset_class"]: int(r["count"]) for _, r in selected_breakdown.iterrows()}
        if selected_breakdown_dict != expected_breakdown:
            raise RuntimeError(f"selected_breakdown inesperado: {selected_breakdown_dict} != {expected_breakdown}")

        # Step 6 compare expected list
        expected_lines = [ln.strip() for ln in expected_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        expected_set = set(expected_lines)
        only_selected = sorted(selected_set - expected_set)
        only_expected = sorted(expected_set - selected_set)
        order_ok = selected_sorted == expected_lines
        exact_ok = order_ok and (len(only_selected) == 0) and (len(only_expected) == 0)
        diff_lines = ["PASS" if exact_ok else "FAIL"]
        if not exact_ok:
            diff_lines.extend(["--- only_in_selected ---", *only_selected, "--- only_in_expected ---", *only_expected, "--- order_equal ---", str(order_ok)])
        write_text(evidence_dir / "selected_vs_expected_diff.txt", "\n".join(diff_lines) + "\n")
        if not exact_ok:
            raise RuntimeError("selected_set difere de expected_tickers_file")

        # Step 7 history audit (ROW_EXISTENCE_ONLY)
        cal_hist = sorted(
            df_daily.loc[(df_daily["date"] >= history_anchor) & (df_daily["date"] <= dataset_end), "date"]
            .drop_duplicates()
            .tolist()
        )
        cal_hist_set = set(cal_hist)
        row_dates_by_ticker = (
            df_daily[df_daily["date"].isin(cal_hist_set)]
            .groupby("ticker", dropna=False)["date"]
            .apply(lambda s: set(pd.to_datetime(s).dt.normalize().tolist()))
            .to_dict()
        )

        hist_rows: list[dict[str, Any]] = []
        hist_fail_lines: list[str] = []
        for ticker in selected_sorted:
            first_date = pd.Timestamp(first_date_map[ticker]).normalize()
            eff_start = max(history_anchor, first_date)
            cal_t = [d for d in cal_hist if d >= eff_start]
            req = len(cal_t)
            present_set = row_dates_by_ticker.get(ticker, set())
            present = sum(1 for d in cal_t if d in present_set)
            missing = req - present

            if missing > 0 and len(hist_fail_lines) < 25:
                miss_sample = [d.strftime("%Y-%m-%d") for d in cal_t if d not in present_set][:10]
                hist_fail_lines.append(f"{ticker}: missing_days_count={missing}; sample={miss_sample}")

            asset_class = selected_df.loc[selected_df["ticker"] == ticker, "asset_class"].iloc[0]
            hist_rows.append(
                {
                    "ticker": ticker,
                    "asset_class": asset_class,
                    "first_date": first_date.strftime("%Y-%m-%d"),
                    "last_date": pd.Timestamp(last_date_map[ticker]).normalize().strftime("%Y-%m-%d"),
                    "history_anchor_date": history_anchor.strftime("%Y-%m-%d"),
                    "history_effective_start_date": eff_start.strftime("%Y-%m-%d"),
                    "history_required_days_count": req,
                    "history_present_days_count": present,
                    "history_missing_days_count": missing,
                    "history_presence_definition": "ROW_EXISTENCE_ONLY",
                }
            )

        hist_df = pd.DataFrame(hist_rows).sort_values("ticker").reset_index(drop=True)
        hist_df.to_csv(evidence_dir / "history_audit_per_ticker.csv", index=False)
        write_text(
            evidence_dir / "history_audit_failures_sample.txt",
            "\n".join(hist_fail_lines) + ("\n" if hist_fail_lines else "NO_HISTORY_AUDIT_FAILURES\n"),
        )

        # Step 8 SSOT output
        source_paths = json.dumps(
            [
                str(base_daily_path),
                str(base_cov_path),
                str(expected_file),
            ],
            ensure_ascii=False,
        )
        ssot_df = hist_df[
            [
                "ticker",
                "asset_class",
                "first_date",
                "last_date",
                "history_anchor_date",
                "history_effective_start_date",
                "history_required_days_count",
                "history_present_days_count",
                "history_missing_days_count",
            ]
        ].copy()
        ssot_df = ssot_df.rename(
            columns={
                "history_anchor_date": "history_anchor_date",
                "history_effective_start_date": "history_effective_start_date",
                "history_required_days_count": "history_required_days_count",
                "history_present_days_count": "history_present_days_count",
                "history_missing_days_count": "history_missing_days_count",
            }
        )
        ssot_df["baseline_ok_k60_n3"] = True
        ssot_df["window_start"] = window_start.strftime("%Y-%m-%d")
        ssot_df["window_end"] = dataset_end.strftime("%Y-%m-%d")
        ssot_df["window_W"] = window_w
        ssot_df["sig_col"] = sig_col
        ssot_df["selection_version"] = "v2"
        ssot_df["created_at_utc"] = generated_at
        ssot_df["source_paths"] = source_paths

        ssot_parquet = out_root / "ssot_approved_tickers.parquet"
        ssot_df.to_parquet(ssot_parquet, index=False)

        md_lines = [
            "# SSOT Approved Tickers (448) - Window K60/N3 + History Audit V2",
            "",
            f"- generated_at_utc: `{generated_at}`",
            f"- selected_count: `{selected_count}`",
            f"- window_start: `{window_start.strftime('%Y-%m-%d')}`",
            f"- window_W: `{window_w}`",
            f"- sig_col: `{sig_col}`",
            "- history_audit_presence_definition: `ROW_EXISTENCE_ONLY`",
            "",
            "## Breakdown",
        ]
        for _, r in selected_breakdown.iterrows():
            md_lines.append(f"- {r['asset_class']}: {int(r['count'])}")
        md_lines.extend(["", "## Tickers (sorted)"])
        md_lines.extend([f"- {t}" for t in selected_sorted])
        write_text(out_root / "ssot_approved_tickers.md", "\n".join(md_lines) + "\n")

        # Step 9 visual update
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
  S005["S005 SSOT tickers aprovados (448)"]
  S001 --> S002 --> S003 --> S004 --> S005
```
"""
        write_text(checklist_path, checklist_content)

        decisions_flow = """flowchart TB
  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]
  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]
  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]
  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]
  S005["S005 SSOT tickers aprovados (448)"]
  S001 --> S002 --> S003 --> S004 --> S005
"""
        write_text(visual_dir / "decisions_flow.mmd", decisions_flow)

        # Step 10 deprecations note
        dep_lines = [
            "# Deprecations Note (ACTIVE chain)",
            "",
            "Itens antigos removidos da cadeia ACTIVE, preservados para auditoria/referência:",
            "",
            "- S005 anterior: base_operacional_v1",
            "- S006 anterior: burners_ranking_spc_v2",
            "",
            "Ação:",
            "- Nenhum artefato deletado.",
            "- Pacotes antigos mantidos em outputs/ para rastreabilidade.",
        ]
        write_text(evidence_dir / "deprecations_note.md", "\n".join(dep_lines) + "\n")

        run_ok = True
    except Exception as exc:
        write_text(evidence_dir / "run_exception.txt", f"{type(exc).__name__}: {exc}\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "evidence": str(evidence_dir / ("run_exception.txt" if not run_ok else "selected_448_tickers_sorted.txt"))}

    # S4 verify selection
    s4_ok = False
    s4_conditions: list[bool] = []
    if run_ok:
        baseline_count = int(pd.read_csv(evidence_dir / "baseline_ok_557_breakdown.csv")["count"].sum())
        win_summary = pd.read_csv(evidence_dir / "window_calendar_summary.csv").iloc[0]
        w_ok = int(win_summary["window_W"]) == expected_w
        selected_df_chk = pd.read_csv(evidence_dir / "selected_448_breakdown.csv")
        selected_breakdown_chk = {r["asset_class"]: int(r["count"]) for _, r in selected_df_chk.iterrows()}
        selected_count_chk = int(sum(selected_breakdown_chk.values()))
        diff_first = (evidence_dir / "selected_vs_expected_diff.txt").read_text(encoding="utf-8").splitlines()[0].strip()
        diff_ok = diff_first == "PASS"
        s4_conditions = [
            baseline_count == expected_baseline_ok_count,
            w_ok,
            selected_count_chk == expected_selected_count,
            selected_breakdown_chk == expected_breakdown,
            diff_ok,
        ]
        s4_ok = all(s4_conditions)
    gates["S4_VERIFY_SELECTION"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5 verify history audit artifacts
    s5_ok = False
    s5_conditions: list[bool] = []
    if run_ok:
        hist_path = evidence_dir / "history_audit_per_ticker.csv"
        hist_exists = hist_path.exists()
        presence_ok = False
        ssot_fields_ok = False
        if hist_exists:
            hist_df_chk = pd.read_csv(hist_path)
            presence_ok = "history_presence_definition" in hist_df_chk.columns and set(hist_df_chk["history_presence_definition"].dropna().unique().tolist()) == {"ROW_EXISTENCE_ONLY"}
        ssot_path = out_root / "ssot_approved_tickers.parquet"
        if ssot_path.exists():
            ssot_cols = set(pd.read_parquet(ssot_path).columns.tolist())
            required_ssot_cols = {
                "ticker",
                "asset_class",
                "first_date",
                "last_date",
                "baseline_ok_k60_n3",
                "window_start",
                "window_end",
                "window_W",
                "sig_col",
                "history_anchor_date",
                "history_effective_start_date",
                "history_required_days_count",
                "history_present_days_count",
                "history_missing_days_count",
                "selection_version",
                "created_at_utc",
                "source_paths",
            }
            ssot_fields_ok = required_ssot_cols.issubset(ssot_cols)
        s5_conditions = [hist_exists, presence_ok, ssot_fields_ok]
        s5_ok = all(s5_conditions)
    gates["S5_VERIFY_HISTORY_AUDIT_ARTIFACTS"] = {"pass": s5_ok, "conditions": s5_conditions}

    # S6 required outputs + hashes
    required_rel = spec["outputs"]["required_files"]
    required_abs = [out_root / rel for rel in required_rel]
    all_required_exist = all(p.exists() for p in required_abs)
    hashes: dict[str, str] = {}
    for p in required_abs:
        if p.exists() and p.is_file():
            hashes[str(p)] = sha256_file(p)

    # report
    overall = all(v.get("pass") is True for v in gates.values())
    report_lines = [
        "# Report - SSOT Approved Tickers 448 with History Audit V2",
        "",
        f"- task_id: `{spec['task_id']}`",
        f"- generated_at_utc: `{generated_at}`",
        f"- overall: `{'PASS' if overall else 'FAIL'}`",
        "",
        "## Gates",
        f"- S1_GATE_ALLOWLIST: `{'PASS' if gates['S1_GATE_ALLOWLIST']['pass'] else 'FAIL'}`",
        f"- S2_CHECK_COMPILE: `{'PASS' if gates['S2_CHECK_COMPILE']['pass'] else 'FAIL'}`",
        f"- S3_RUN: `{'PASS' if gates['S3_RUN']['pass'] else 'FAIL'}`",
        f"- S4_VERIFY_SELECTION: `{'PASS' if gates['S4_VERIFY_SELECTION']['pass'] else 'FAIL'}`",
        f"- S5_VERIFY_HISTORY_AUDIT_ARTIFACTS: `{'PASS' if gates['S5_VERIFY_HISTORY_AUDIT_ARTIFACTS']['pass'] else 'FAIL'}`",
    ]
    write_text(out_root / "report.md", "\n".join(report_lines) + "\n")
    hashes[str(out_root / "report.md")] = sha256_file(out_root / "report.md")

    s6_ok = all_required_exist and (len(hashes) >= len([p for p in required_abs if p.is_file()]))
    gates["S6_VERIFY_OUTPUTS_AND_HASHES"] = {"pass": s6_ok, "required_exists": all_required_exist}
    overall = all(v.get("pass") is True for v in gates.values())

    manifest = {
        "task_id": spec["task_id"],
        "generated_at_utc": generated_at,
        "task_spec_path": str(spec_path),
        "output_root": str(out_root),
        "gates": gates,
        "overall": "PASS" if overall else "FAIL",
        "required_files": [str(p) for p in required_abs],
        "hashes_sha256": hashes,
    }
    write_json(out_root / "manifest.json", manifest)

    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

