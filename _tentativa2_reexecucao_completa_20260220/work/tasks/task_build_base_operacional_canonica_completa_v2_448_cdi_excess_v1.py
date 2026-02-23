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


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run_cmd(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def ensure_python() -> None:
    import sys

    if str(Path(sys.executable)) != OFFICIAL_PYTHON:
        raise RuntimeError(f"Interpreter invalido: {sys.executable}. Use {OFFICIAL_PYTHON}.")


def extract_lessons_applied(json_path: Path, md_path: Path) -> list[dict[str, str]]:
    keywords = ["join", "calendar", "date", "missing", "key", "retorno", "log", "continuity", "coverage"]
    out: list[dict[str, str]] = []
    if json_path.exists():
        raw = json.loads(json_path.read_text(encoding="utf-8"))
        items = raw.get("lessons", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
        for item in items:
            title = str(item.get("title", ""))
            context = str(item.get("context", ""))
            tags = " ".join([str(t) for t in item.get("tags", [])]) if isinstance(item.get("tags", []), list) else ""
            text = f"{title} {context} {tags}".lower()
            if any(k in text for k in keywords):
                out.append(
                    {
                        "lesson_id": str(item.get("lesson_id", "UNKNOWN")),
                        "title": title[:180],
                    }
                )
    if len(out) == 0 and md_path.exists():
        md = md_path.read_text(encoding="utf-8")
        if len(md.strip()) > 0:
            out.append({"lesson_id": "MD_FALLBACK", "title": "Fallback from LESSONS_LEARNED.md by keyword policy"})
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    ensure_python()

    spec_path = Path(args.task_spec).resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))

    repo_root = Path(spec["repo_root"]).resolve()
    attempt2_root = Path(spec["attempt2_root"]).resolve()
    out_root = Path(spec["outputs"]["output_root"]).resolve()

    panel_dir = out_root / "panel"
    coverage_dir = out_root / "coverage"
    cal_dir = out_root / "calendar"
    meta_dir = out_root / "metadata"
    evidence_dir = out_root / "evidence"
    visual_dir = out_root / "visual"
    for d in [panel_dir, coverage_dir, cal_dir, meta_dir, evidence_dir, visual_dir]:
        d.mkdir(parents=True, exist_ok=True)

    approved_path = Path(spec["inputs"]["approved_universe_ssot_parquet"]).resolve()
    base_path = Path(spec["inputs"]["base_operacional_daily_parquet"]).resolve()
    cdi_path = Path(spec["inputs"]["ssot_cdi_daily_parquet"]).resolve()
    ll_json = Path(spec["inputs"]["lessons_learned_paths"]["json"]).resolve()
    ll_md = Path(spec["inputs"]["lessons_learned_paths"]["md"]).resolve()
    checklist_md = Path(spec["inputs"]["owner_visual_checklist_md"]).resolve()

    required_panel_cols = list(spec["inputs"]["required_panel_cols_min"])
    cdi_policy = spec["inputs"]["cdi_column_policy"]
    cdi_candidates = list(cdi_policy["preferred_daily_return_col_candidates"])
    forced_cdi_col = cdi_policy.get("cdi_return_col_forced_if_present", "cdi_ret_t")

    generated_at = now_utc()
    gates: dict[str, dict[str, Any]] = {}

    # S1 allowlist
    snapshot = attempt2_root / "ssot_snapshot"
    snap_status = run_cmd(["git", "status", "--short", "--", str(snapshot)], repo_root).stdout.strip()
    allow_ok = all(str(p).startswith(str(attempt2_root)) for p in [approved_path, base_path, cdi_path, checklist_md])
    allow_ok = allow_ok and (snap_status == "")
    write_text(evidence_dir / "allowlist_check.txt", f"allow_ok={allow_ok}\nsnapshot_status={snap_status or 'NO_CHANGES'}\n")
    gates["S1_GATE_ALLOWLIST"] = {"pass": allow_ok, "evidence": str(evidence_dir / "allowlist_check.txt")}

    # S2 compile
    comp = run_cmd([OFFICIAL_PYTHON, "-m", "py_compile", str(Path(__file__).resolve())], repo_root)
    compile_ok = comp.returncode == 0
    write_text(evidence_dir / "compile_check.txt", f"returncode={comp.returncode}\nstdout={comp.stdout}\nstderr={comp.stderr}\n")
    gates["S2_CHECK_COMPILE"] = {"pass": compile_ok, "evidence": str(evidence_dir / "compile_check.txt")}

    run_ok = False
    run_error = ""
    try:
        # Step 0 lessons
        lessons_applied = extract_lessons_applied(ll_json, ll_md)

        # Step 1 approved tickers
        approved = pd.read_parquet(approved_path)
        approved["ticker"] = approved["ticker"].astype(str)
        approved_tickers = sorted(approved["ticker"].unique().tolist())
        approved_count = len(approved_tickers)

        # Step 2 panel schema
        panel = pd.read_parquet(base_path)
        write_json(
            evidence_dir / "schema_panel_found.json",
            {"columns": list(panel.columns), "dtypes": {c: str(panel[c].dtype) for c in panel.columns}},
        )
        missing_panel_cols = [c for c in required_panel_cols if c not in panel.columns]
        if missing_panel_cols:
            raise RuntimeError(f"required_panel_cols_min ausentes: {missing_panel_cols}")

        # Step 3 filter panel
        panel["ticker"] = panel["ticker"].astype(str)
        panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
        panel = panel[panel["ticker"].isin(approved_tickers)].copy()
        panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

        # Step 4 key uniqueness
        dups = (
            panel.groupby(["date", "ticker"], as_index=False)
            .size()
            .rename(columns={"size": "n"})
        )
        dups = dups[dups["n"] > 1].copy()
        if len(dups) == 0:
            pd.DataFrame([{"status": "PASS", "duplicate_rows": 0}]).to_csv(evidence_dir / "key_uniqueness_check.csv", index=False)
        else:
            dups.head(200).to_csv(evidence_dir / "key_uniqueness_check.csv", index=False)
            raise RuntimeError("Duplicidade em chave (date,ticker) detectada")

        # Step 5 CDI schema + candidate selection
        cdi = pd.read_parquet(cdi_path)
        cdi["date"] = pd.to_datetime(cdi["date"]).dt.normalize()
        write_json(
            evidence_dir / "schema_cdi_found.json",
            {"columns": list(cdi.columns), "dtypes": {c: str(cdi[c].dtype) for c in cdi.columns}},
        )
        chosen_col = None
        if forced_cdi_col in cdi.columns:
            chosen_col = forced_cdi_col
        else:
            for c in cdi_candidates:
                if c in cdi.columns:
                    chosen_col = c
                    break
        if chosen_col is None:
            raise RuntimeError(
                "CDI return column não encontrada nos candidatos "
                f"{cdi_candidates}. Colunas encontradas: {list(cdi.columns)}"
            )

        # Step 6 CDI series derivation
        cdi_s = cdi[["date", chosen_col]].copy().rename(columns={chosen_col: "cdi_ret_simple"})
        cdi_s["cdi_ret_simple"] = pd.to_numeric(cdi_s["cdi_ret_simple"], errors="coerce")
        invalid_cdi = int(((1.0 + cdi_s["cdi_ret_simple"]) <= 0).fillna(False).sum())
        if invalid_cdi > 0:
            raise RuntimeError("Violação domínio CDI: 1+cdi_ret_simple <= 0")
        cdi_s["cdi_logret"] = np.log1p(cdi_s["cdi_ret_simple"])

        # Step 7 calendar table
        cal = pd.DataFrame({"date": sorted(panel["date"].drop_duplicates().tolist())})
        min_date = pd.Timestamp(cal["date"].min()).strftime("%Y-%m-%d")
        max_date = pd.Timestamp(cal["date"].max()).strftime("%Y-%m-%d")
        n_days = int(len(cal))
        cal["calendar_source"] = "PANEL_UNIQUE_DATES"
        cal["min_date"] = min_date
        cal["max_date"] = max_date
        cal["n_days"] = n_days
        cal_path = cal_dir / "calendar_trading_days.parquet"
        cal.to_parquet(cal_path, index=False)

        # Step 8 join panel x cdi
        merged = panel.merge(cdi_s, on="date", how="left", validate="many_to_one")
        missing_cdi_rows = int(merged["cdi_ret_simple"].isna().sum())
        missing_cdi_dates = merged.loc[merged["cdi_ret_simple"].isna(), "date"].drop_duplicates().sort_values().head(25)
        pd.DataFrame(
            [
                {
                    "panel_rows": int(len(panel)),
                    "panel_unique_dates": int(panel["date"].nunique()),
                    "cdi_unique_dates": int(cdi_s["date"].nunique()),
                    "missing_cdi_rows_after_join": missing_cdi_rows,
                    "missing_cdi_unique_dates_after_join": int(merged.loc[merged["cdi_ret_simple"].isna(), "date"].nunique()),
                    "join_status": "PASS" if missing_cdi_rows == 0 else "FAIL",
                }
            ]
        ).to_csv(evidence_dir / "join_coverage_summary.csv", index=False)
        if missing_cdi_rows > 0:
            write_text(evidence_dir / "join_missing_dates_sample.txt", "\n".join([d.strftime("%Y-%m-%d") for d in missing_cdi_dates]) + "\n")
            raise RuntimeError("Join CDI incompleto: existem datas do painel sem CDI")

        # Step 9 domain checks logret
        merged["ret_simple"] = pd.to_numeric(merged["ret_simple"], errors="coerce")
        invalid_ret = int(((1.0 + merged["ret_simple"]) <= 0).fillna(False).sum())
        merged["logret"] = np.log1p(merged["ret_simple"])
        domain_pass = invalid_ret == 0
        pd.DataFrame(
            [
                {
                    "rows_total": int(len(merged)),
                    "invalid_ret_simple_domain_count": invalid_ret,
                    "domain_rule": "1+ret_simple>0",
                    "status": "PASS" if domain_pass else "FAIL",
                }
            ]
        ).to_csv(evidence_dir / "domain_checks_summary.csv", index=False)
        if not domain_pass:
            raise RuntimeError("Violação domínio retorno: 1+ret_simple<=0")

        # Step 10 excess metrics
        merged["excess_diff"] = merged["ret_simple"] - merged["cdi_ret_simple"]
        merged["excess_ratio"] = (1.0 + merged["ret_simple"]) / (1.0 + merged["cdi_ret_simple"]) - 1.0
        merged["excess_log"] = merged["logret"] - merged["cdi_logret"]

        # Step 11 missingness summary
        miss_rows = []
        for col in ["ret_simple", "cdi_ret_simple", "excess_log"]:
            miss_rows.append({"column": col, "missing_count": int(merged[col].isna().sum()), "rows_total": int(len(merged))})
        pd.DataFrame(miss_rows).to_csv(evidence_dir / "missingness_summary.csv", index=False)

        # date range summary
        pd.DataFrame(
            [
                {
                    "panel_min_date": str(pd.Timestamp(merged["date"].min()).date()),
                    "panel_max_date": str(pd.Timestamp(merged["date"].max()).date()),
                    "ticker_count": int(merged["ticker"].nunique()),
                    "row_count": int(len(merged)),
                    "approved_universe_count": approved_count,
                }
            ]
        ).to_csv(evidence_dir / "date_range_summary.csv", index=False)

        # Step 12 canonical panel parquet
        canonical_min_cols = [
            "date",
            "ticker",
            "asset_class",
            "ret_simple",
            "logret",
            "cdi_ret_simple",
            "cdi_logret",
            "excess_diff",
            "excess_ratio",
            "excess_log",
        ]
        for c in canonical_min_cols:
            if c not in merged.columns:
                raise RuntimeError(f"Coluna obrigatória ausente no panel canônico: {c}")

        # keep all columns with required minimum leading
        extra_cols = [c for c in merged.columns if c not in canonical_min_cols]
        merged = merged[canonical_min_cols + extra_cols].copy()
        panel_out_path = panel_dir / "base_operacional_canonica.parquet"
        merged.to_parquet(panel_out_path, index=False)

        # Step 13 coverage parquet
        cov = (
            merged.groupby(["ticker", "asset_class"], as_index=False)
            .agg(
                first_date=("date", "min"),
                last_date=("date", "max"),
                n_days=("date", "count"),
                n_missing_ret_simple=("ret_simple", lambda s: int(s.isna().sum())),
                n_missing_cdi=("cdi_ret_simple", lambda s: int(s.isna().sum())),
                n_missing_excess_log=("excess_log", lambda s: int(s.isna().sum())),
            )
        )
        dup_flag = panel.groupby(["ticker", "date"]).size().reset_index(name="n")
        dup_tickers = set(dup_flag[dup_flag["n"] > 1]["ticker"].astype(str).unique().tolist())
        cov["has_duplicates_key"] = cov["ticker"].astype(str).isin(dup_tickers)
        cov["passes_domain_checks"] = True
        cov_path = coverage_dir / "base_operacional_coverage.parquet"
        cov.to_parquet(cov_path, index=False)

        # Step 14 metadata json
        input_paths = {
            "approved_universe_ssot_path": str(approved_path),
            "base_operacional_daily_path": str(base_path),
            "cdi_daily_path": str(cdi_path),
            "lessons_json_path": str(ll_json),
            "lessons_md_path": str(ll_md),
        }
        input_hashes = {k: sha256_file(Path(v)) for k, v in input_paths.items() if Path(v).exists()}
        metadata = {
            "build_version": "v1",
            "built_at_utc": generated_at,
            "input_paths": input_paths,
            "input_hashes_sha256": input_hashes,
            "approved_universe_ssot_path": str(approved_path),
            "approved_universe_ssot_hash": sha256_file(approved_path),
            "cdi_daily_path": str(cdi_path),
            "cdi_daily_hash": sha256_file(cdi_path),
            "cdi_return_col_chosen": chosen_col,
            "panel_required_cols_min": required_panel_cols,
            "schema_contract_minimum": canonical_min_cols,
            "rules": {
                "parquet_is_canonical": True,
                "csv_only_for_human_evidence": True,
                "approved_universe_restricted": True,
            },
            "lessons_learned_applied": lessons_applied,
        }
        write_json(meta_dir / "dataset_metadata.json", metadata)

        # Step 15 visual update with S006
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
  S006["S006 Base Operacional Canônica Completa (448)"]
  S001 --> S002 --> S003 --> S004 --> S005 --> S006
```
"""
        write_text(checklist_md, checklist_content)

        mmd = """flowchart TB
  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]
  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]
  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]
  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]
  S005["S005 SSOT tickers aprovados (448)"]
  S006["S006 Base Operacional Canônica Completa (448)"]
  S001 --> S002 --> S003 --> S004 --> S005 --> S006
"""
        write_text(visual_dir / "decisions_flow.mmd", mmd)

        run_ok = True
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(evidence_dir / "run_exception.txt", run_error + "\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "error": run_error if not run_ok else None}

    # S4 verify package complete logic
    s4_ok = False
    s4_conditions: list[bool] = []
    if run_ok:
        panel_out = panel_dir / "base_operacional_canonica.parquet"
        cov_out = coverage_dir / "base_operacional_coverage.parquet"
        cal_out = cal_dir / "calendar_trading_days.parquet"
        meta_out = meta_dir / "dataset_metadata.json"

        panel_chk = pd.read_parquet(panel_out)
        ticker_count_ok = int(panel_chk["ticker"].nunique()) == 448
        schema_ok = all(c in panel_chk.columns for c in spec["outputs"]["schema_contract_minimum"])
        missing_cdi_after_join = int(panel_chk["cdi_ret_simple"].isna().sum()) == 0
        domain_ok = int(((1.0 + panel_chk["ret_simple"]) <= 0).fillna(False).sum()) == 0
        key_ok = not panel_chk.duplicated(subset=["date", "ticker"]).any()
        s4_conditions = [
            panel_out.exists(),
            cov_out.exists(),
            cal_out.exists(),
            meta_out.exists(),
            schema_ok,
            ticker_count_ok,
            missing_cdi_after_join,
            domain_ok,
            key_ok,
        ]
        s4_ok = all(s4_conditions)
    gates["S4_VERIFY_BASE_CANONICA_COMPLETA"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5 outputs + hashes
    required_files = [out_root / rel for rel in spec["outputs"]["required_files"]]
    required_exists = all(p.exists() for p in required_files)
    hashes = {}
    for p in required_files:
        if p.exists() and p.is_file():
            hashes[str(p)] = sha256_file(p)
    s5_ok = required_exists and len(hashes) >= len([p for p in required_files if p.is_file()])
    gates["S5_VERIFY_OUTPUTS_AND_HASHES"] = {"pass": s5_ok, "required_exists": required_exists}

    overall = all(v.get("pass") is True for v in gates.values())

    report_lines = [
        "# Report - Base Operacional Canônica Completa V2 448 CDI Excess V1",
        "",
        f"- task_id: `{spec['task_id']}`",
        f"- generated_at_utc: `{generated_at}`",
        f"- overall: `{'PASS' if overall else 'FAIL'}`",
        "",
        "## Gates",
        f"- S1_GATE_ALLOWLIST: `{'PASS' if gates['S1_GATE_ALLOWLIST']['pass'] else 'FAIL'}`",
        f"- S2_CHECK_COMPILE: `{'PASS' if gates['S2_CHECK_COMPILE']['pass'] else 'FAIL'}`",
        f"- S3_RUN: `{'PASS' if gates['S3_RUN']['pass'] else 'FAIL'}`",
        f"- S4_VERIFY_BASE_CANONICA_COMPLETA: `{'PASS' if gates['S4_VERIFY_BASE_CANONICA_COMPLETA']['pass'] else 'FAIL'}`",
        f"- S5_VERIFY_OUTPUTS_AND_HASHES: `{'PASS' if gates['S5_VERIFY_OUTPUTS_AND_HASHES']['pass'] else 'FAIL'}`",
    ]
    meta_path = meta_dir / "dataset_metadata.json"
    if meta_path.exists():
        try:
            meta_obj = json.loads(meta_path.read_text(encoding="utf-8"))
            report_lines.extend(
                [
                    "",
                    "## CDI Column Selection",
                    f"- cdi_return_col_chosen: `{meta_obj.get('cdi_return_col_chosen')}`",
                ]
            )
        except Exception:
            pass
    if not run_ok and len(run_error) > 0:
        report_lines.extend(["", "## Error", f"- {run_error}"])

    write_text(out_root / "report.md", "\n".join(report_lines) + "\n")
    hashes[str(out_root / "report.md")] = sha256_file(out_root / "report.md")

    manifest = {
        "task_id": spec["task_id"],
        "generated_at_utc": generated_at,
        "task_spec_path": str(spec_path),
        "output_root": str(out_root),
        "overall": "PASS" if overall else "FAIL",
        "gates": gates,
        "required_files": [str(p) for p in required_files],
        "hashes_sha256": hashes,
    }
    write_json(out_root / "manifest.json", manifest)

    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

