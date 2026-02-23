from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go


OFFICIAL_PYTHON = "/home/wilson/PortfolioZero/.venv/bin/python"


def now_utc() -> str:
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


def ensure_python() -> None:
    import sys
    if str(Path(sys.executable)) != OFFICIAL_PYTHON:
        raise RuntimeError(f"Interpreter invalido: {sys.executable}. Use {OFFICIAL_PYTHON}.")


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
    artifacts_dir = out_root / "artifacts"
    evidence_dir = out_root / "evidence"
    visual_dir = out_root / "visual"
    for d in [artifacts_dir, evidence_dir, visual_dir]:
        d.mkdir(parents=True, exist_ok=True)

    ranking_path = Path(spec["inputs"]["ranking_daily_parquet"]).resolve()
    checklist_path = Path(spec["inputs"]["checklist_path"]).resolve()
    cfg = spec["inputs"]["config"]
    season_start = pd.Timestamp(cfg["season_start"]).normalize()
    season_end = pd.Timestamp(cfg["season_end"]).normalize()
    score_col = cfg["score_col"]
    points_table = list(cfg["points_table_top10_f1"])
    top_n_lines = int(cfg["plotly"]["top_lines_final_standings_n"])
    html_filename = cfg["plotly"]["output_html_filename"]

    generated_at = now_utc()
    gates: dict[str, dict[str, Any]] = {}

    # S1 allowlist + non-modification of snapshot/checklist
    snapshot = attempt2_root / "ssot_snapshot"
    snap_hash_before = sha256_file(checklist_path) if checklist_path.exists() else None
    s1_ok = str(ranking_path).startswith(str(attempt2_root))
    s1_ok = s1_ok and str(out_root).startswith(str(attempt2_root / "outputs"))
    s1_ok = s1_ok and snapshot.exists()
    write_text(evidence_dir / "allowlist_check.txt", f"allow_ok={s1_ok}\nchecklist_hash_before={snap_hash_before}\n")
    gates["S1_GATE_ALLOWLIST"] = {"pass": s1_ok}

    # S2 compile
    import subprocess

    comp = subprocess.run(
        [OFFICIAL_PYTHON, "-m", "py_compile", str(Path(__file__).resolve())],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )
    s2_ok = comp.returncode == 0
    write_text(evidence_dir / "compile_check.txt", f"returncode={comp.returncode}\nstdout={comp.stdout}\nstderr={comp.stderr}\n")
    gates["S2_CHECK_COMPILE"] = {"pass": s2_ok}

    run_ok = False
    run_error = ""
    try:
        df = pd.read_parquet(ranking_path)
        df["decision_date"] = pd.to_datetime(df["decision_date"]).dt.normalize()

        season_df = df[(df["decision_date"] >= season_start) & (df["decision_date"] <= season_end)].copy()
        if len(season_df) == 0:
            raise RuntimeError("Sem dados na janela da temporada 2018H2")

        effective_season_end = pd.Timestamp(season_df["decision_date"].max()).normalize()
        decision_days = sorted(season_df["decision_date"].drop_duplicates().tolist())

        # Step 2: points awarded daily
        awarded_rows = []
        tie_days = 0

        for d in decision_days:
            day = season_df[season_df["decision_date"] == d].copy()
            day = day.sort_values([score_col, "ticker"], ascending=[False, True]).reset_index(drop=True)
            if len(day) < 10:
                cutoff_score = day[score_col].iloc[-1]
            else:
                cutoff_score = day[score_col].iloc[9]

            included = day[day[score_col] >= cutoff_score].copy()
            if len(included) > 10:
                tie_days += 1
            included = included.sort_values([score_col, "ticker"], ascending=[False, True]).reset_index(drop=True)
            included["rank_position"] = included.index + 1
            included["cutoff_score"] = cutoff_score
            included["included_by_cutoff_tie"] = included["rank_position"] > 10

            pts = []
            for _, r in included.iterrows():
                pos = int(r["rank_position"])
                if pos <= 10:
                    pts.append(points_table[pos - 1])
                else:
                    pts.append(points_table[9])  # ponto da posição 10 para empatados além do corte
            included["points_awarded"] = pts
            included["score"] = included[score_col]

            cols_keep = [
                "decision_date",
                "ticker",
                "asset_class",
                "score",
                "rank_position",
                "points_awarded",
                "cutoff_score",
                "included_by_cutoff_tie",
            ]
            awarded_rows.append(included[cols_keep])

        awarded = pd.concat(awarded_rows, ignore_index=True)
        points_awarded_path = artifacts_dir / "points_awarded_daily.parquet"
        awarded.to_parquet(points_awarded_path, index=False)

        # Step 3 cumulative
        all_pairs = (
            pd.MultiIndex.from_product([decision_days, sorted(awarded["ticker"].unique())], names=["decision_date", "ticker"])
            .to_frame(index=False)
        )
        cum = all_pairs.merge(awarded[["decision_date", "ticker", "points_awarded"]], on=["decision_date", "ticker"], how="left")
        cum["points_awarded"] = cum["points_awarded"].fillna(0).astype(float)
        cum = cum.sort_values(["ticker", "decision_date"])
        cum["points_cumulative"] = cum.groupby("ticker")["points_awarded"].cumsum()
        points_cum_path = artifacts_dir / "points_cumulative_daily.parquet"
        cum[["decision_date", "ticker", "points_cumulative"]].to_parquet(points_cum_path, index=False)

        # Step 4 final standings
        season_end_df = awarded[awarded["decision_date"] <= effective_season_end].copy()
        final = (
            season_end_df.groupby("ticker", as_index=False)
            .agg(
                points_total=("points_awarded", "sum"),
                n_days_scored=("decision_date", "nunique"),
                n_wins_rank1=("rank_position", lambda s: int((s == 1).sum())),
                first_day_scored=("decision_date", "min"),
                last_day_scored=("decision_date", "max"),
                asset_class=("asset_class", "first"),
            )
        )
        final["season_end_date"] = effective_season_end
        final = final.sort_values(["points_total", "ticker"], ascending=[False, True]).reset_index(drop=True)
        final_path = artifacts_dir / "final_standings_2018.parquet"
        final[
            [
                "season_end_date",
                "ticker",
                "points_total",
                "n_days_scored",
                "n_wins_rank1",
                "first_day_scored",
                "last_day_scored",
                "asset_class",
            ]
        ].to_parquet(final_path, index=False)

        # Step 5 evidence
        pd.DataFrame(
            [
                {
                    "season_start": str(season_start.date()),
                    "season_end": str(season_end.date()),
                    "effective_season_end": str(effective_season_end.date()),
                    "n_decision_days": int(len(decision_days)),
                }
            ]
        ).to_csv(evidence_dir / "season_context.csv", index=False)

        pd.DataFrame([{"days_with_more_than_10_included_due_to_cutoff_tie": int(tie_days)}]).to_csv(
            evidence_dir / "tie_cutoff_days_count.csv", index=False
        )

        first5 = decision_days[:5]
        sample = awarded[awarded["decision_date"].isin(first5)].copy()
        sample = sample.sort_values(["decision_date", "rank_position", "ticker"]).reset_index(drop=True)
        sample.to_csv(evidence_dir / "top10_sample_first5days.csv", index=False)

        # Step 6 plotly
        top_tickers = final.head(top_n_lines)["ticker"].tolist()
        cum_top = cum[cum["ticker"].isin(top_tickers)].copy()

        fig = go.Figure()
        for t in top_tickers:
            tdf = cum_top[cum_top["ticker"] == t]
            fig.add_trace(go.Scatter(x=tdf["decision_date"], y=tdf["points_cumulative"], mode="lines", name=t))
        fig.update_layout(title="Championship 2018H2 - Cumulative Points", xaxis_title="Decision date", yaxis_title="Points cumulative")

        fig2 = go.Figure()
        bar_df = final.head(top_n_lines)
        fig2.add_trace(go.Bar(x=bar_df["ticker"], y=bar_df["points_total"], name="Final points"))
        fig2.update_layout(title="Championship 2018H2 - Final Standings (Top)", xaxis_title="Ticker", yaxis_title="Points total")

        html_path = visual_dir / html_filename
        html = (
            "<html><head><meta charset='utf-8'></head><body>\n"
            + fig.to_html(include_plotlyjs="cdn", full_html=False)
            + "<hr/>\n"
            + fig2.to_html(include_plotlyjs=False, full_html=False)
            + "\n</body></html>"
        )
        write_text(html_path, html)

        # Step 7 report
        top20 = final.head(20)
        report_lines = [
            "# Report - Experimento Championship F1 from S007V2 2018H2 Plotly V1",
            "",
            f"- generated_at_utc: `{generated_at}`",
            f"- season_start: `{season_start.date()}`",
            f"- season_end: `{season_end.date()}`",
            f"- effective_season_end: `{effective_season_end.date()}`",
            f"- n_decision_days: `{len(decision_days)}`",
            f"- tie_policy_include_all_at_cutoff: `True`",
            f"- tie_days_more_than_10_included: `{tie_days}`",
            "",
            "## Top-20 final standings (ticker + points)",
        ]
        for i, (_, r) in enumerate(top20.iterrows(), start=1):
            report_lines.append(f"{i:02d}. {r['ticker']} - {float(r['points_total']):.0f}")
        write_text(out_root / "report.md", "\n".join(report_lines) + "\n")

        run_ok = True
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(evidence_dir / "run_exception.txt", run_error + "\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "error": run_error if not run_ok else None}

    required_abs = [out_root / rel for rel in spec["outputs"]["required_files"]]
    manifest_path = out_root / "manifest.json"

    # Primeiro write provisório do manifest para cumprir required_files na validação S4.
    write_json(
        manifest_path,
        {
            "task_id": spec["task_id"],
            "generated_at_utc": generated_at,
            "status": "provisional_manifest_before_s4_s5",
        },
    )

    # S4 outputs
    s4_conditions = [
        all(p.exists() for p in required_abs),
        (artifacts_dir / "final_standings_2018.parquet").exists() and len(pd.read_parquet(artifacts_dir / "final_standings_2018.parquet")) > 0 if run_ok else False,
        (visual_dir / html_filename).exists() and (visual_dir / html_filename).stat().st_size > 0 if (visual_dir / html_filename).exists() else False,
        (evidence_dir / "season_context.csv").exists(),
    ]
    s4_ok = all(s4_conditions)
    gates["S4_VERIFY_OUTPUTS"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5 hashes + checklist unchanged check
    hashes: dict[str, str] = {}
    for p in required_abs:
        if p.exists() and p.is_file() and p.name != "manifest.json":
            hashes[str(p)] = sha256_file(p)

    checklist_hash_after = sha256_file(checklist_path) if checklist_path.exists() else None
    checklist_unchanged = (checklist_hash_after == snap_hash_before)
    write_text(
        evidence_dir / "checklist_unchanged_check.txt",
        f"checklist_hash_before={snap_hash_before}\nchecklist_hash_after={checklist_hash_after}\nunchanged={checklist_unchanged}\n",
    )
    s5_ok = all(p.exists() for p in required_abs) and checklist_unchanged
    gates["S5_VERIFY_HASHES"] = {"pass": s5_ok, "checklist_unchanged": checklist_unchanged}

    overall = all(v.get("pass") is True for v in gates.values())
    manifest = {
        "task_id": spec["task_id"],
        "generated_at_utc": generated_at,
        "task_spec_path": str(spec_path),
        "output_root": str(out_root),
        "overall": "PASS" if overall else "FAIL",
        "gates": gates,
        "required_files": [str(p) for p in required_abs],
        "hashes_sha256": hashes,
    }
    write_json(manifest_path, manifest)
    hashes[str(manifest_path)] = sha256_file(manifest_path)
    manifest["hashes_sha256"] = hashes
    write_json(manifest_path, manifest)

    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

