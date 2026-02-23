from __future__ import annotations

import argparse
import hashlib
import json
import re
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


def ols_slope(y: np.ndarray) -> float:
    n = int(y.shape[0])
    if n < 2:
        return 0.0
    x = np.arange(n, dtype=float)
    xm = float(x.mean())
    ym = float(y.mean())
    den = float(np.sum((x - xm) ** 2))
    if den == 0.0:
        return 0.0
    num = float(np.sum((x - xm) * (y - ym)))
    return num / den


def maybe_update_checklist(checklist_path: Path) -> bool:
    if not checklist_path.exists():
        return False
    original = checklist_path.read_text(encoding="utf-8")
    updated = original

    if 'S008["S008 Candidatos: Campeonato F1 + slope_45 (filtro slope_60>0)"]' not in updated:
        updated = updated.replace(
            '  S007["S007 Ranking Burners diário OEE LP/CP (448) — V2 warmup buffer"]',
            '  S007["S007 Ranking Burners diário OEE LP/CP (448) — V2 warmup buffer"]\n'
            '  S008["S008 Candidatos: Campeonato F1 + slope_45 (filtro slope_60>0)"]',
        )
    updated = updated.replace(
        "  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007",
        "  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007 --> S008",
    )
    updated = re.sub(r"^last_updated: .*$", f"last_updated: {datetime.now(timezone.utc).date().isoformat()}", updated, flags=re.MULTILINE)

    if updated != original:
        checklist_path.write_text(updated, encoding="utf-8")
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    ensure_python()

    spec_path = Path(args.task_spec).resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    generated_at = now_utc()

    repo_root = Path(spec["repo_root"]).resolve()
    attempt2_root = Path(spec["attempt2_root"]).resolve()
    ranking_path = Path(spec["inputs"]["ranking_daily_parquet"]).resolve()
    checklist_path = Path(spec["inputs"]["owner_visual_checklist_md"]).resolve()
    cfg = spec["inputs"]["config"]

    output_root = Path(spec["outputs"]["output_root"]).resolve()
    candidates_dir = output_root / "candidates"
    championship_dir = output_root / "championship"
    slopes_dir = output_root / "slopes"
    metadata_dir = output_root / "metadata"
    evidence_dir = output_root / "evidence"
    visual_dir = output_root / "visual"
    for d in [candidates_dir, championship_dir, slopes_dir, metadata_dir, evidence_dir, visual_dir]:
        d.mkdir(parents=True, exist_ok=True)

    gates: dict[str, dict[str, Any]] = {}
    checklist_hash_before = sha256_file(checklist_path) if checklist_path.exists() else None

    # S1 allowlist
    s1_ok = True
    s1_ok = s1_ok and str(ranking_path).startswith(str(attempt2_root))
    s1_ok = s1_ok and str(output_root).startswith(str(attempt2_root / "outputs"))
    s1_ok = s1_ok and str(checklist_path).startswith(str(attempt2_root / "work"))
    s1_ok = s1_ok and (attempt2_root / "ssot_snapshot").exists()
    gates["S1_GATE_ALLOWLIST"] = {"pass": s1_ok}

    # S2 compile
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
    checklist_updated = False
    try:
        df = pd.read_parquet(ranking_path)
        required_cols = {"decision_date", "data_end_date", "ticker", "asset_class", cfg["score_col"]}
        missing = sorted(required_cols - set(df.columns))
        if missing:
            raise RuntimeError(f"Colunas obrigatorias ausentes no ranking: {missing}")

        score_col = cfg["score_col"]
        topk_points = int(cfg["topk_daily_points"])
        topk_pool = int(cfg["topk_pool"])
        points_table = list(cfg["points_table_top10_f1"])
        cutoff_pos_excess = int(cfg["tie_policy"]["cutoff_points_position_for_excess"])
        windows = [int(w) for w in cfg["slopes"]["windows"]]
        primary_window = int(cfg["slopes"]["primary_window"])
        alive_window = int(cfg["slopes"]["alive_filter_window"])

        df["decision_date"] = pd.to_datetime(df["decision_date"]).dt.normalize()
        df["data_end_date"] = pd.to_datetime(df["data_end_date"]).dt.normalize()
        df = df.sort_values(["decision_date", score_col, "ticker"], ascending=[True, False, True]).reset_index(drop=True)

        decision_days = sorted(df["decision_date"].drop_duplicates().tolist())
        universe_tickers = sorted(df["ticker"].drop_duplicates().tolist())
        first_data_end = (
            df.groupby("decision_date", as_index=False)["data_end_date"].first().sort_values("decision_date").reset_index(drop=True)
        )

        # Step 2.1 points awarded daily
        awarded_rows: list[pd.DataFrame] = []
        tie_days = 0
        for d in decision_days:
            day = df[df["decision_date"] == d].copy()
            day = day.sort_values([score_col, "ticker"], ascending=[False, True]).reset_index(drop=True)
            if len(day) == 0:
                continue
            cutoff_score = day[score_col].iloc[min(topk_points - 1, len(day) - 1)]
            inc = day[day[score_col] >= cutoff_score].copy().reset_index(drop=True)
            inc["rank_position"] = np.arange(1, len(inc) + 1, dtype=int)
            inc["cutoff_score"] = cutoff_score
            inc["included_by_cutoff_tie"] = inc["rank_position"] > topk_points
            if len(inc) > topk_points:
                tie_days += 1
            inc["points_awarded"] = inc["rank_position"].apply(
                lambda p: float(points_table[p - 1]) if p <= topk_points else float(points_table[cutoff_pos_excess - 1])
            )
            inc["oee_overall"] = inc[score_col].astype(float)
            awarded_rows.append(
                inc[
                    [
                        "decision_date",
                        "data_end_date",
                        "ticker",
                        "asset_class",
                        "oee_overall",
                        "rank_position",
                        "points_awarded",
                        "cutoff_score",
                        "included_by_cutoff_tie",
                    ]
                ]
            )
        points_awarded = pd.concat(awarded_rows, ignore_index=True) if awarded_rows else pd.DataFrame()
        points_awarded.to_parquet(championship_dir / "points_awarded_daily.parquet", index=False)

        # Step 2.2 cumulative points for all tickers over all decision days
        grid = pd.MultiIndex.from_product([decision_days, universe_tickers], names=["decision_date", "ticker"]).to_frame(index=False)
        grid = grid.merge(first_data_end, on="decision_date", how="left")
        pts_day = points_awarded[["decision_date", "ticker", "points_awarded"]] if len(points_awarded) else pd.DataFrame(columns=["decision_date", "ticker", "points_awarded"])
        cumulative = grid.merge(pts_day, on=["decision_date", "ticker"], how="left")
        cumulative["points_awarded"] = cumulative["points_awarded"].fillna(0.0).astype(float)
        cumulative = cumulative.sort_values(["ticker", "decision_date"]).reset_index(drop=True)
        cumulative["points_total"] = cumulative.groupby("ticker")["points_awarded"].cumsum()
        cumulative[["decision_date", "data_end_date", "ticker", "points_total"]].to_parquet(
            championship_dir / "points_cumulative_daily.parquet", index=False
        )

        # Step 2.3 standings daily
        standings_rows: list[pd.DataFrame] = []
        for d in decision_days:
            day = cumulative[cumulative["decision_date"] == d][["decision_date", "data_end_date", "ticker", "points_total"]].copy()
            day = day.sort_values(["points_total", "ticker"], ascending=[False, True]).reset_index(drop=True)
            day["rank_points_total"] = day["points_total"].rank(method="min", ascending=False).astype(int)
            standings_rows.append(day)
        standings_daily = pd.concat(standings_rows, ignore_index=True)
        standings_daily.to_parquet(championship_dir / "standings_daily.parquet", index=False)

        # Precomputo vetorial para acelerar calculo de slope (evita filtros repetidos por DataFrame).
        cumulative_wide = (
            cumulative[["decision_date", "ticker", "points_total"]]
            .pivot(index="decision_date", columns="ticker", values="points_total")
            .sort_index()
        )
        day_index = {d: i for i, d in enumerate(cumulative_wide.index.tolist())}
        ticker_series = {t: cumulative_wide[t].to_numpy(dtype=float) for t in cumulative_wide.columns}

        # Step 2.5/2.6/2.7 slopes and ranking by slope_45 among alive
        slopes_rows: list[dict[str, Any]] = []
        candidates_rows: list[pd.DataFrame] = []
        for d in decision_days:
            day_st = standings_daily[standings_daily["decision_date"] == d].copy()
            day_st = day_st.sort_values(["points_total", "ticker"], ascending=[False, True]).reset_index(drop=True)
            cutoff_pool = day_st["points_total"].iloc[min(topk_pool - 1, len(day_st) - 1)] if len(day_st) else 0.0
            pool = day_st[day_st["points_total"] >= cutoff_pool].copy()
            pool_tickers = set(pool["ticker"].tolist())

            slope_day_rows = []
            end_idx = day_index[d]
            for t in pool["ticker"].tolist():
                series = ticker_series[t]
                rec: dict[str, Any] = {
                    "decision_date": d,
                    "ticker": t,
                    "points_total": float(series[end_idx]),
                }
                for w in windows:
                    start_idx = max(0, end_idx - w + 1)
                    y = series[start_idx : end_idx + 1]
                    rec[f"slope_{w}"] = float(ols_slope(y))
                rec["alive_slope60"] = bool(rec.get(f"slope_{alive_window}", 0.0) > 0.0)
                slope_day_rows.append(rec)
            if slope_day_rows:
                slope_day = pd.DataFrame(slope_day_rows)
                slope_day = slope_day.sort_values([f"slope_{primary_window}", "ticker"], ascending=[False, True]).reset_index(drop=True)
                alive_mask = slope_day["alive_slope60"] == True
                slope_day["rank_slope45"] = np.nan
                if alive_mask.any():
                    slope_day.loc[alive_mask, "rank_slope45"] = np.arange(1, int(alive_mask.sum()) + 1, dtype=int)
                for r in slope_day.to_dict(orient="records"):
                    slopes_rows.append(r)
            else:
                slope_day = pd.DataFrame(columns=["ticker", "alive_slope60", "rank_slope45", "slope_30", "slope_45", "slope_60"])

            # Step 2.8 candidates_daily for all tickers in day
            day_rank = df[df["decision_date"] == d][["decision_date", "data_end_date", "ticker", "asset_class", score_col]].copy()
            day_rank = day_rank.rename(columns={score_col: "oee_overall"})
            day_cand = day_rank.merge(
                day_st[["ticker", "points_total", "rank_points_total"]],
                on="ticker",
                how="left",
            )
            if len(slope_day):
                day_cand = day_cand.merge(
                    slope_day[["ticker", "slope_30", "slope_45", "slope_60", "alive_slope60", "rank_slope45"]],
                    on="ticker",
                    how="left",
                )
            else:
                day_cand["slope_30"] = np.nan
                day_cand["slope_45"] = np.nan
                day_cand["slope_60"] = np.nan
                day_cand["alive_slope60"] = False
                day_cand["rank_slope45"] = np.nan
            day_cand["in_pool_top15"] = day_cand["ticker"].isin(pool_tickers)
            # Evita warning de downcasting de fillna em pandas recente.
            day_cand["alive_slope60"] = day_cand["alive_slope60"].eq(True)
            day_cand["is_selected_candidate"] = day_cand["in_pool_top15"] & day_cand["alive_slope60"]
            candidates_rows.append(day_cand)

        slopes_pool_daily = pd.DataFrame(slopes_rows)
        if len(slopes_pool_daily) == 0:
            slopes_pool_daily = pd.DataFrame(columns=["decision_date", "ticker", "points_total", "slope_30", "slope_45", "slope_60", "alive_slope60", "rank_slope45"])
        slopes_pool_daily.to_parquet(slopes_dir / "slopes_pool_daily.parquet", index=False)

        candidates_daily = pd.concat(candidates_rows, ignore_index=True) if candidates_rows else pd.DataFrame()
        ordered_cols = [
            "decision_date",
            "data_end_date",
            "ticker",
            "asset_class",
            "oee_overall",
            "points_total",
            "rank_points_total",
            "in_pool_top15",
            "slope_30",
            "slope_45",
            "slope_60",
            "alive_slope60",
            "rank_slope45",
            "is_selected_candidate",
        ]
        candidates_daily = candidates_daily[ordered_cols]
        candidates_daily.to_parquet(candidates_dir / "candidates_daily.parquet", index=False)

        # metadata / evidence / visual
        metadata = {
            "task_id": spec["task_id"],
            "generated_at_utc": generated_at,
            "n_decision_days": int(len(decision_days)),
            "n_tickers_universe": int(len(universe_tickers)),
            "score_col": score_col,
            "topk_daily_points": topk_points,
            "topk_pool": topk_pool,
            "slopes_windows": windows,
            "primary_window": primary_window,
            "alive_filter_rule": cfg["slopes"]["alive_filter_rule"],
            "temporal_rule": cfg["temporal_rule"],
            "candidates_daily_scope": "all_tickers_in_ranking_daily_with_pool_and_selection_flags",
        }
        write_json(metadata_dir / "dataset_metadata.json", metadata)

        first_day = min(decision_days) if decision_days else None
        sample = candidates_daily[candidates_daily["decision_date"] == first_day].copy()
        sample = sample[sample["in_pool_top15"] == True].sort_values(["rank_points_total", "ticker"])
        sample.to_csv(evidence_dir / "sample_first_selection_day.csv", index=False)

        pd.DataFrame([{"days_with_tie_at_daily_points_cutoff": int(tie_days)}]).to_csv(
            evidence_dir / "tie_cutoff_days_count.csv", index=False
        )

        mmd = "\n".join(
            [
                "flowchart TB",
                '  S007["S007 Ranking Burners diario OEE LP/CP (448) — V2 warmup buffer"]',
                '  S008["S008 Candidatos: Campeonato F1 + slope_45 (filtro slope_60>0)"]',
                "  S007 --> S008",
            ]
        )
        write_text(visual_dir / "decisions_flow.mmd", mmd + "\n")

        report_lines = [
            "# Report - S008 Candidates Championship + Slope45 V1",
            "",
            f"- generated_at_utc: `{generated_at}`",
            f"- decision_days: `{len(decision_days)}`",
            f"- tickers_universe: `{len(universe_tickers)}`",
            f"- temporal_rule: `{cfg['temporal_rule']}`",
            "- tie_policy_daily_points: `include_all_at_cutoff=true; excesso recebe pontos da posicao 10 (1 ponto)`",
            "- pool_rule: `POOL(D) = top15 por points_total com inclusao de empatados no cutoff`",
            "- slopes_rule: `slope_45 principal; filtro alive slope_60>0; slope_30 diagnostico`",
            "- candidates_scope: `candidates_daily contem todos os tickers do ranking diario; flags e ranks definem pool/selecionados`",
            "",
            "## Top-20 no ultimo dia por points_total",
        ]
        if len(standings_daily):
            last_day = max(decision_days)
            top20 = standings_daily[standings_daily["decision_date"] == last_day].sort_values(
                ["points_total", "ticker"], ascending=[False, True]
            ).head(20)
            for i, (_, r) in enumerate(top20.iterrows(), start=1):
                report_lines.append(f"{i:02d}. {r['ticker']} - points_total={float(r['points_total']):.0f}")
        write_text(output_root / "report.md", "\n".join(report_lines) + "\n")

        run_ok = True
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(evidence_dir / "run_exception.txt", run_error + "\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "error": None if run_ok else run_error}

    required_abs = [output_root / rel for rel in spec["outputs"]["required_files"]]
    manifest_path = output_root / "manifest.json"
    write_json(manifest_path, {"task_id": spec["task_id"], "status": "provisional"})

    # S4 outputs
    s4_conditions = [
        all(p.exists() for p in required_abs),
        (candidates_dir / "candidates_daily.parquet").exists()
        and len(pd.read_parquet(candidates_dir / "candidates_daily.parquet")) > 0
        if run_ok
        else False,
        (output_root / "report.md").exists()
        and ("data_end_date = previous trading day" in (output_root / "report.md").read_text(encoding="utf-8"))
        and ("tie_policy_daily_points" in (output_root / "report.md").read_text(encoding="utf-8"))
        and ("pool_rule" in (output_root / "report.md").read_text(encoding="utf-8"))
        and ("slopes_rule" in (output_root / "report.md").read_text(encoding="utf-8")),
        (evidence_dir / "sample_first_selection_day.csv").exists()
        and len(pd.read_csv(evidence_dir / "sample_first_selection_day.csv")) > 0
        if (evidence_dir / "sample_first_selection_day.csv").exists()
        else False,
    ]
    s4_ok = all(s4_conditions)
    gates["S4_VERIFY_OUTPUTS"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5 hashes
    hashes: dict[str, str] = {}
    for p in required_abs:
        if p.exists() and p.is_file() and p.name != "manifest.json":
            hashes[str(p)] = sha256_file(p)
    s5_ok = all(p.exists() for p in required_abs)
    gates["S5_VERIFY_HASHES"] = {"pass": s5_ok}

    # OVERALL + checklist update after PASS only
    overall = all(v.get("pass") is True for v in gates.values())
    if overall:
        checklist_updated = maybe_update_checklist(checklist_path)
    checklist_hash_after = sha256_file(checklist_path) if checklist_path.exists() else None

    manifest = {
        "task_id": spec["task_id"],
        "generated_at_utc": generated_at,
        "task_spec_path": str(spec_path),
        "output_root": str(output_root),
        "overall": "PASS" if overall else "FAIL",
        "gates": gates,
        "required_files": [str(p) for p in required_abs],
        "hashes_sha256": hashes,
        "checklist_update_after_pass": {
            "checklist_path": str(checklist_path),
            "updated": bool(checklist_updated),
            "hash_before": checklist_hash_before,
            "hash_after": checklist_hash_after,
        },
    }
    write_json(manifest_path, manifest)
    hashes[str(manifest_path)] = sha256_file(manifest_path)
    manifest["hashes_sha256"] = hashes
    write_json(manifest_path, manifest)

    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

