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


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_cmd(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def ensure_python() -> None:
    import sys
    if str(Path(sys.executable)) != OFFICIAL_PYTHON:
        raise RuntimeError(f"Interpreter invalido: {sys.executable}. Use {OFFICIAL_PYTHON}.")


def prefix(arr: np.ndarray) -> np.ndarray:
    out = np.zeros(len(arr), dtype=float)
    if len(arr) > 0:
        np.cumsum(arr.astype(float), out=out)
    return out


def win_sum(pref: np.ndarray, l: int, r: int) -> float:
    if r < l:
        return 0.0
    if l <= 0:
        return float(pref[r])
    return float(pref[r] - pref[l - 1])


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

    ranking_dir = out_root / "ranking"
    cep_dir = out_root / "cep_states"
    baseline_dir = out_root / "baseline_limits"
    meta_dir = out_root / "metadata"
    evidence_dir = out_root / "evidence"
    visual_dir = out_root / "visual"
    for d in [ranking_dir, cep_dir, baseline_dir, meta_dir, evidence_dir, visual_dir]:
        d.mkdir(parents=True, exist_ok=True)

    panel_path = Path(spec["inputs"]["panel_parquet"]).resolve()
    calendar_path = Path(spec["inputs"]["calendar_parquet"]).resolve()
    approved_path = Path(spec["inputs"]["approved_universe_ssot_parquet"]).resolve()
    checklist_path = Path(spec["inputs"]["owner_visual_checklist_md"]).resolve()
    ranking_v1_root = Path(spec["inputs"]["ranking_v1_output_root"]).resolve()

    cfg = spec["inputs"]["config"]
    x_col = cfg["signal_col_x"]
    warmup_start = pd.Timestamp(cfg["warmup_start"]).normalize()
    warmup_end = pd.Timestamp(cfg["warmup_end"]).normalize()
    first_selection_month = str(cfg["first_selection_month"])
    cp_window = int(cfg["cp_window_sessions"])
    n = int(cfg["cep"]["N"])
    k = int(cfg["cep"]["K"])
    a2 = float(cfg["cep"]["spc_constants"]["A2_for_N3"])
    d3 = float(cfg["cep"]["spc_constants"]["D3_for_N3"])
    d4 = float(cfg["cep"]["spc_constants"]["D4_for_N3"])
    warmup_buffer = int(cfg["warmup_buffer_sessions"])

    generated_at = now_utc()
    gates: dict[str, dict[str, Any]] = {}

    # S1
    snapshot = attempt2_root / "ssot_snapshot"
    snap_status = run_cmd(["git", "status", "--short", "--", str(snapshot)], repo_root).stdout.strip()
    s1_ok = all(str(p).startswith(str(attempt2_root)) for p in [panel_path, calendar_path, approved_path, checklist_path])
    s1_ok = s1_ok and (snap_status == "")
    write_text(evidence_dir / "allowlist_check.txt", f"allow_ok={s1_ok}\nsnapshot_status={snap_status or 'NO_CHANGES'}\n")
    gates["S1_GATE_ALLOWLIST"] = {"pass": s1_ok}

    # S2
    comp = run_cmd([OFFICIAL_PYTHON, "-m", "py_compile", str(Path(__file__).resolve())], repo_root)
    s2_ok = comp.returncode == 0
    write_text(evidence_dir / "compile_check.txt", f"returncode={comp.returncode}\nstdout={comp.stdout}\nstderr={comp.stderr}\n")
    gates["S2_CHECK_COMPILE"] = {"pass": s2_ok}

    run_ok = False
    run_error = ""
    first_decision_date = None
    try:
        approved = pd.read_parquet(approved_path)[["ticker", "asset_class"]].copy()
        approved["ticker"] = approved["ticker"].astype(str)
        approved_tickers = sorted(approved["ticker"].unique().tolist())
        if len(approved_tickers) != 448:
            raise RuntimeError(f"approved universe size inesperado: {len(approved_tickers)} != 448")
        ticker_asset = dict(zip(approved["ticker"], approved["asset_class"]))

        panel = pd.read_parquet(panel_path).copy()
        panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
        panel["ticker"] = panel["ticker"].astype(str)
        if x_col not in panel.columns:
            raise RuntimeError(f"signal_col_x ausente: {x_col}")
        panel[x_col] = pd.to_numeric(panel[x_col], errors="coerce")
        panel = panel[panel["ticker"].isin(approved_tickers)].copy()
        panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

        cal_df = pd.read_parquet(calendar_path).copy()
        cal_df["date"] = pd.to_datetime(cal_df["date"]).dt.normalize()
        calendar = sorted(cal_df["date"].drop_duplicates().tolist())
        cal_set = set(calendar)
        cal_pos = {d: i for i, d in enumerate(calendar)}

        panel_dates = sorted([d for d in panel["date"].drop_duplicates().tolist() if d in cal_set])
        if len(panel_dates) == 0:
            raise RuntimeError("Sem panel_dates após filtro")

        warmup_last = max([d for d in panel_dates if d <= warmup_end], default=None)
        if warmup_last is None:
            raise RuntimeError("Sem warmup_last_trading_day <= warmup_end")
        warmup_last_idx = cal_pos[warmup_last]
        first_decision_candidates = [d for d in panel_dates if d.strftime("%Y-%m").startswith(first_selection_month)]
        if len(first_decision_candidates) == 0:
            raise RuntimeError(f"Sem first_decision_date no mês {first_selection_month}")
        first_decision_date = min(first_decision_candidates)
        first_decision_idx = cal_pos[first_decision_date]

        # maps
        ticker_first = panel.groupby("ticker", as_index=False)["date"].min().rename(columns={"date": "first_date"})
        ticker_first_map = dict(zip(ticker_first["ticker"], ticker_first["first_date"]))
        ticker_data = {t: g[["date", x_col]].copy().sort_values("date") for t, g in panel.groupby("ticker", sort=False)}

        # baseline + cep
        base_rows: list[dict[str, Any]] = []
        cep_rows: list[dict[str, Any]] = []
        baseline_std_map: dict[str, float] = {}
        baseline_insufficient = 0

        for t in approved_tickers:
            asset_class = ticker_asset.get(t, "UNKNOWN")
            g = ticker_data.get(t)
            if g is None or len(g) == 0:
                baseline_insufficient += 1
                base_rows.append(
                    {
                        "ticker": t,
                        "asset_class": asset_class,
                        "baseline_start": None,
                        "baseline_end": None,
                        "xbarbar": np.nan,
                        "Rbar": np.nan,
                        "LCL_Xbar": np.nan,
                        "UCL_Xbar": np.nan,
                        "LCL_R": np.nan,
                        "UCL_R": np.nan,
                        "eligible_from_date": None,
                        "baseline_insufficient": True,
                    }
                )
                continue

            t_dates = g["date"].drop_duplicates().sort_values().tolist()
            first_date_i = pd.Timestamp(t_dates[0]).normalize()
            preexisting = first_date_i <= warmup_last

            if preexisting:
                idx_end = warmup_last_idx - warmup_buffer
                if idx_end < 0:
                    idx_end = 0
                baseline_end = calendar[idx_end]
            else:
                baseline_end = t_dates[61] if len(t_dates) >= 62 else None

            if baseline_end is None:
                baseline_insufficient += 1
                base_rows.append(
                    {
                        "ticker": t,
                        "asset_class": asset_class,
                        "baseline_start": None,
                        "baseline_end": None,
                        "xbarbar": np.nan,
                        "Rbar": np.nan,
                        "LCL_Xbar": np.nan,
                        "UCL_Xbar": np.nan,
                        "LCL_R": np.nan,
                        "UCL_R": np.nan,
                        "eligible_from_date": None,
                        "baseline_insufficient": True,
                    }
                )
                continue

            eligible_dates = [d for d in t_dates if d <= baseline_end]
            if len(eligible_dates) < 62:
                baseline_insufficient += 1
                base_rows.append(
                    {
                        "ticker": t,
                        "asset_class": asset_class,
                        "baseline_start": None,
                        "baseline_end": str(pd.Timestamp(baseline_end).date()),
                        "xbarbar": np.nan,
                        "Rbar": np.nan,
                        "LCL_Xbar": np.nan,
                        "UCL_Xbar": np.nan,
                        "LCL_R": np.nan,
                        "UCL_R": np.nan,
                        "eligible_from_date": None,
                        "baseline_insufficient": True,
                    }
                )
                continue

            bw_dates = eligible_dates[-62:]
            baseline_start = bw_dates[0]
            x_map = dict(zip(g["date"], g[x_col]))
            x_vals = np.array([x_map.get(d, np.nan) for d in bw_dates], dtype=float)
            if np.isnan(x_vals).any():
                baseline_insufficient += 1
                base_rows.append(
                    {
                        "ticker": t,
                        "asset_class": asset_class,
                        "baseline_start": str(pd.Timestamp(baseline_start).date()),
                        "baseline_end": str(pd.Timestamp(baseline_end).date()),
                        "xbarbar": np.nan,
                        "Rbar": np.nan,
                        "LCL_Xbar": np.nan,
                        "UCL_Xbar": np.nan,
                        "LCL_R": np.nan,
                        "UCL_R": np.nan,
                        "eligible_from_date": None,
                        "baseline_insufficient": True,
                    }
                )
                continue

            xbars = []
            rs = []
            for j in range(k):
                w = x_vals[j : j + n]
                xbars.append(float(np.mean(w)))
                rs.append(float(np.max(w) - np.min(w)))
            xbarbar = float(np.mean(xbars))
            rbar = float(np.mean(rs))
            lcl_x = xbarbar - a2 * rbar
            ucl_x = xbarbar + a2 * rbar
            lcl_r = d3 * rbar
            ucl_r = d4 * rbar
            baseline_std_map[t] = float(np.std(x_vals, ddof=1))

            # NEW rule: preexisting ticker -> eligible_from baseline_end + buffer sessions
            idx_end = cal_pos[pd.Timestamp(baseline_end).normalize()]
            if preexisting:
                idx_eligible = min(idx_end + warmup_buffer, len(calendar) - 1)
                eligible_from = calendar[idx_eligible]
            else:
                # unchanged for post-warmup IPO: first date with 3 valid calendar sessions
                eligible_from = None
                for i in range(idx_end, len(calendar)):
                    if i - 2 < 0:
                        continue
                    d2, d1, d0 = calendar[i - 2], calendar[i - 1], calendar[i]
                    if d2 in x_map and d1 in x_map and d0 in x_map:
                        xv = [x_map[d2], x_map[d1], x_map[d0]]
                        if all(pd.notna(v) for v in xv):
                            eligible_from = d0
                            break

            base_rows.append(
                {
                    "ticker": t,
                    "asset_class": asset_class,
                    "baseline_start": str(pd.Timestamp(baseline_start).date()),
                    "baseline_end": str(pd.Timestamp(baseline_end).date()),
                    "xbarbar": xbarbar,
                    "Rbar": rbar,
                    "LCL_Xbar": lcl_x,
                    "UCL_Xbar": ucl_x,
                    "LCL_R": lcl_r,
                    "UCL_R": ucl_r,
                    "eligible_from_date": str(pd.Timestamp(eligible_from).date()) if eligible_from is not None else None,
                    "baseline_insufficient": False,
                }
            )

            # CEP states
            if eligible_from is not None:
                start_idx = cal_pos[pd.Timestamp(eligible_from).normalize()]
                for i in range(start_idx, len(calendar)):
                    if i - 2 < 0:
                        continue
                    d2, d1, d0 = calendar[i - 2], calendar[i - 1], calendar[i]
                    if not (d2 in x_map and d1 in x_map and d0 in x_map):
                        continue
                    xv = [x_map[d2], x_map[d1], x_map[d0]]
                    if not all(pd.notna(v) for v in xv):
                        continue
                    xbar3 = float(np.mean(xv))
                    r3 = float(np.max(xv) - np.min(xv))
                    level_out = (xbar3 < lcl_x) or (xbar3 > ucl_x)
                    var_out = r3 > ucl_r
                    if level_out:
                        state = "OUT_OF_CONTROL_LEVEL"
                    elif var_out:
                        state = "OUT_OF_CONTROL_VAR"
                    else:
                        state = "IN_CONTROL"
                    eta = 1 if state == "IN_CONTROL" else 0
                    cep_rows.append(
                        {
                            "date": str(pd.Timestamp(d0).date()),
                            "ticker": t,
                            "x": float(x_map[d0]),
                            "xbar_3": xbar3,
                            "R_3": r3,
                            "LCL_Xbar": lcl_x,
                            "UCL_Xbar": ucl_x,
                            "UCL_R": ucl_r,
                            "state": state,
                            "eta": eta,
                        }
                    )

        baseline_df = pd.DataFrame(base_rows)
        baseline_out = baseline_dir / "baseline_limits_by_ticker.parquet"
        baseline_df.to_parquet(baseline_out, index=False)

        cep_df = pd.DataFrame(cep_rows)
        if len(cep_df) == 0:
            raise RuntimeError("CEP states vazio")
        cep_df["date"] = pd.to_datetime(cep_df["date"]).dt.normalize()
        cep_df = cep_df.sort_values(["ticker", "date"]).reset_index(drop=True)
        cep_out = cep_dir / "cep_states_daily.parquet"
        cep_df.to_parquet(cep_out, index=False)

        # ranking daily (optimized cumulative + baseline std fallback)
        panel_idx = panel[["date", "ticker", x_col]].copy()
        panel_idx["cal_idx"] = panel_idx["date"].map(cal_pos).astype(int)
        panel_idx = panel_idx.dropna(subset=["cal_idx"]).copy()
        cep_idx = cep_df[["date", "ticker", "eta", "state"]].copy()
        cep_idx["cal_idx"] = cep_idx["date"].map(cal_pos).astype(int)
        cep_idx = cep_idx.dropna(subset=["cal_idx"]).copy()

        rows = []
        decision_dates = [d for d in panel_dates if d >= first_decision_date]
        for t in approved_tickers:
            asset_class = ticker_asset.get(t, "UNKNOWN")
            first_i = ticker_first_map[t]
            start_i = max(warmup_start, first_i)
            start_idx = cal_pos.get(start_i, None)
            if start_idx is None:
                start_idx = next((i for i, dd in enumerate(calendar) if dd >= start_i), None)
            if start_idx is None:
                continue

            x_arr = np.full(len(calendar), np.nan, dtype=float)
            eta_arr = np.zeros(len(calendar), dtype=float)
            state_arr = np.array(["NO_STATE"] * len(calendar), dtype=object)

            gp = panel_idx[panel_idx["ticker"] == t]
            if len(gp) > 0:
                x_arr[gp["cal_idx"].to_numpy()] = gp[x_col].to_numpy(dtype=float)
            gc = cep_idx[cep_idx["ticker"] == t]
            if len(gc) > 0:
                idxs = gc["cal_idx"].to_numpy(dtype=int)
                eta_arr[idxs] = gc["eta"].to_numpy(dtype=float)
                state_arr[idxs] = gc["state"].astype(str).to_numpy()

            x_clean = np.where(np.isnan(x_arr), 0.0, x_arr)
            pref_eta = prefix(eta_arr)
            pref_pos = prefix(eta_arr * np.maximum(x_clean, 0.0))
            pref_abs = prefix(eta_arr * np.abs(x_clean))
            active = (eta_arr == 1.0) & (~np.isnan(x_arr))
            pref_cnt = prefix(active.astype(float))
            pref_sum = prefix(np.where(active, x_arr, 0.0))
            pref_sumsq = prefix(np.where(active, x_arr * x_arr, 0.0))
            fallback_v = baseline_std_map.get(t, np.nan)

            for d in decision_dates:
                dec_idx = cal_pos[d]
                data_end_idx = dec_idx - 1
                if data_end_idx < start_idx:
                    continue
                n_lp = data_end_idx - start_idx + 1
                n_cp = min(cp_window, n_lp)
                l_lp, r_lp = start_idx, data_end_idx
                l_cp, r_cp = r_lp - n_cp + 1, r_lp

                A_lp = win_sum(pref_eta, l_lp, r_lp) / n_lp if n_lp > 0 else 0.0
                den_lp = win_sum(pref_abs, l_lp, r_lp)
                P_lp = win_sum(pref_pos, l_lp, r_lp) / den_lp if den_lp > 0 else 0.0
                c_lp = win_sum(pref_cnt, l_lp, r_lp)
                if c_lp >= 2:
                    s_lp = win_sum(pref_sum, l_lp, r_lp)
                    ss_lp = win_sum(pref_sumsq, l_lp, r_lp)
                    var_lp = (ss_lp - (s_lp * s_lp / c_lp)) / (c_lp - 1)
                    V_lp = float(np.sqrt(max(var_lp, 0.0)))
                else:
                    V_lp = float(fallback_v) if pd.notna(fallback_v) else np.nan

                A_cp = win_sum(pref_eta, l_cp, r_cp) / n_cp if n_cp > 0 else 0.0
                den_cp = win_sum(pref_abs, l_cp, r_cp)
                P_cp = win_sum(pref_pos, l_cp, r_cp) / den_cp if den_cp > 0 else 0.0
                c_cp = win_sum(pref_cnt, l_cp, r_cp)
                if c_cp >= 2:
                    s_cp = win_sum(pref_sum, l_cp, r_cp)
                    ss_cp = win_sum(pref_sumsq, l_cp, r_cp)
                    var_cp = (ss_cp - (s_cp * s_cp / c_cp)) / (c_cp - 1)
                    V_cp = float(np.sqrt(max(var_cp, 0.0)))
                else:
                    V_cp = float(fallback_v) if pd.notna(fallback_v) else np.nan

                eta_end = int(eta_arr[data_end_idx]) if data_end_idx >= 0 else 0
                state_end = str(state_arr[data_end_idx]) if data_end_idx >= 0 else "NO_STATE"
                w_cp = n_cp / n_lp
                w_lp = 1.0 - w_cp

                rows.append(
                    {
                        "decision_date": d,
                        "data_end_date": calendar[data_end_idx],
                        "ticker": t,
                        "asset_class": asset_class,
                        "N_LP": n_lp,
                        "N_CP": n_cp,
                        "w_cp": w_cp,
                        "w_lp": w_lp,
                        "A_LP": A_lp,
                        "P_LP": P_lp,
                        "V_LP": V_lp,
                        "A_CP": A_cp,
                        "P_CP": P_cp,
                        "V_CP": V_cp,
                        "eta_end": eta_end,
                        "state_end": state_end,
                    }
                )

        rank_df = pd.DataFrame(rows)
        if len(rank_df) == 0:
            raise RuntimeError("ranking vazio")
        rank_df["Q_LP"] = 1.0 - rank_df.groupby("decision_date")["V_LP"].rank(pct=True, method="average")
        rank_df["Q_CP"] = 1.0 - rank_df.groupby("decision_date")["V_CP"].rank(pct=True, method="average")
        rank_df.loc[rank_df["V_LP"].isna(), "Q_LP"] = 0.0
        rank_df.loc[rank_df["V_CP"].isna(), "Q_CP"] = 0.0
        rank_df["OEE_LP"] = rank_df["A_LP"] * rank_df["P_LP"] * rank_df["Q_LP"]
        rank_df["OEE_CP"] = rank_df["A_CP"] * rank_df["P_CP"] * rank_df["Q_CP"]
        rank_df["OEE_OVERALL"] = rank_df["w_lp"] * rank_df["OEE_LP"] + rank_df["w_cp"] * rank_df["OEE_CP"]

        rank_df = rank_df[
            [
                "decision_date",
                "data_end_date",
                "ticker",
                "asset_class",
                "N_LP",
                "N_CP",
                "w_cp",
                "w_lp",
                "A_LP",
                "P_LP",
                "V_LP",
                "Q_LP",
                "OEE_LP",
                "A_CP",
                "P_CP",
                "V_CP",
                "Q_CP",
                "OEE_CP",
                "OEE_OVERALL",
                "eta_end",
                "state_end",
            ]
        ].copy()
        rank_df = rank_df.sort_values(["decision_date", "OEE_OVERALL", "ticker"], ascending=[True, False, True]).reset_index(drop=True)
        ranking_out = ranking_dir / "burners_ranking_daily.parquet"
        rank_df.to_parquet(ranking_out, index=False)

        # evidences
        first_day = pd.Timestamp(first_decision_date).normalize()
        first_df = rank_df[rank_df["decision_date"] == first_day].copy()
        nonzero = int((first_df["OEE_OVERALL"].fillna(0) != 0).sum())
        eta1 = int((first_df["eta_end"] == 1).sum())
        std_oee = float(first_df["OEE_OVERALL"].std(ddof=1)) if len(first_df) > 1 else 0.0
        alpha_tie = bool((first_df.sort_values(["OEE_OVERALL", "ticker"], ascending=[False, True]).head(20)["ticker"].tolist()
                          == sorted(first_df["ticker"].tolist())[:20]) and nonzero == 0)
        pd.DataFrame(
            [
                {
                    "first_decision_date": str(first_day.date()),
                    "count_nonzero_OEE_OVERALL": nonzero,
                    "std_OEE_OVERALL": std_oee,
                    "count_eta_end_eq_1": eta1,
                    "top20_pure_alpha_tie": alpha_tie,
                    "accept_nonzero_or_std": bool((std_oee > 0) or (nonzero > 0)),
                    "accept_eta_end": bool(eta1 > 0),
                    "accept_no_alpha_tie": bool(not alpha_tie),
                }
            ]
        ).to_csv(evidence_dir / "first_selection_day_check.csv", index=False)

        top20 = first_df.sort_values(["OEE_OVERALL", "ticker"], ascending=[False, True]).head(20)
        lines = [f"first_decision_date={first_day.date()}"]
        for i, (_, r) in enumerate(top20.iterrows(), start=1):
            lines.append(f"{i:02d}. {r['ticker']} | OEE_OVERALL={r['OEE_OVERALL']:.10f}")
        write_text(evidence_dir / "oee_top20_first_selection.txt", "\n".join(lines) + "\n")

        pd.DataFrame(
            [
                {
                    "min_decision_date": str(pd.Timestamp(rank_df["decision_date"].min()).date()),
                    "max_decision_date": str(pd.Timestamp(rank_df["decision_date"].max()).date()),
                    "rows": int(len(rank_df)),
                    "tickers": int(rank_df["ticker"].nunique()),
                }
            ]
        ).to_csv(evidence_dir / "date_range_summary.csv", index=False)
        pd.DataFrame(
            [
                {
                    "baseline_insufficient_count": int(baseline_insufficient),
                    "baseline_total_tickers": int(len(approved_tickers)),
                    "cep_rows": int(len(cep_df)),
                }
            ]
        ).to_csv(evidence_dir / "eligibility_summary.csv", index=False)
        cep_counts = cep_df.groupby(["date", "state"], as_index=False)["ticker"].nunique().rename(columns={"ticker": "n_tickers"})
        cep_counts.to_csv(evidence_dir / "cep_state_counts.csv", index=False)
        write_text(
            evidence_dir / "deprecation_note_v1.md",
            "\n".join(
                [
                    "# Deprecation note",
                    "",
                    f"- Deprecated package (kept): `{ranking_v1_root}`",
                    f"- Active replacement: `{out_root}`",
                    "- V1 artifacts were preserved; no deletion performed.",
                ]
            )
            + "\n",
        )

        metadata = {
            "build_version": "v2_warmup_buffer",
            "built_at_utc": generated_at,
            "input_paths": {
                "panel": str(panel_path),
                "calendar": str(calendar_path),
                "approved_universe": str(approved_path),
                "deprecated_v1": str(ranking_v1_root),
            },
            "input_hashes_sha256": {
                str(panel_path): sha256_file(panel_path),
                str(calendar_path): sha256_file(calendar_path),
                str(approved_path): sha256_file(approved_path),
            },
            "config": {
                "signal_col_x": x_col,
                "warmup_start": str(warmup_start.date()),
                "warmup_end": str(warmup_end.date()),
                "first_selection_month": first_selection_month,
                "cp_window_sessions": cp_window,
                "cep_N": n,
                "cep_K": k,
                "warmup_buffer_sessions": warmup_buffer,
                "warmup_buffer_rule": cfg["warmup_buffer_rule"],
            },
            "rule_change_summary": "Preexisting tickers now use baseline_end shifted back by warmup_buffer sessions; eligible_from set to baseline_end+buffer to ensure state exists at first data_end.",
            "first_selection_day": str(first_day.date()),
        }
        write_json(meta_dir / "dataset_metadata.json", metadata)

        # Update checklist only after successful materialization
        checklist = """# Checklist — Estado Confirmado (Owner) — CEP_BUNDLE_CORE (Tentativa 2)

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
  S007["S007 Ranking Burners diário OEE LP/CP (448) — V2 warmup buffer"]
  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007
```
"""
        write_text(checklist_path, checklist)
        write_text(
            visual_dir / "decisions_flow.mmd",
            "\n".join(
                [
                    "flowchart TB",
                    '  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]',
                    '  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]',
                    '  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]',
                    '  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]',
                    '  S005["S005 SSOT tickers aprovados (448)"]',
                    '  S006["S006 Base Operacional Canônica Completa (448)"]',
                    '  S007["S007 Ranking Burners diário OEE LP/CP (448) — V2 warmup buffer"]',
                    "  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007",
                    "",
                ]
            ),
        )

        run_ok = True
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(evidence_dir / "run_exception.txt", run_error + "\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "error": run_error if not run_ok else None}

    # S4
    s4_ok = False
    s4_conditions: list[bool] = []
    if run_ok:
        fs = pd.read_csv(evidence_dir / "first_selection_day_check.csv").iloc[0]
        s4_conditions = [
            (evidence_dir / "first_selection_day_check.csv").exists(),
            bool(fs["accept_nonzero_or_std"]),
            bool(fs["accept_eta_end"]),
            bool(fs["accept_no_alpha_tie"]),
        ]
        s4_ok = all(s4_conditions)
    gates["S4_VERIFY_FIRST_SELECTION_NOT_ALL_ZERO"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5
    required_files = [out_root / rel for rel in spec["outputs"]["required_files"]]
    required_exists = all(p.exists() for p in required_files)
    hashes: dict[str, str] = {}
    for p in required_files:
        if p.exists() and p.is_file() and p.name != "manifest.json":
            hashes[str(p)] = sha256_file(p)
    # manifest self-hash é escrito após o arquivo existir, para evitar paradoxos de hash do próprio conteúdo.
    s5_ok = required_exists
    gates["S5_VERIFY_OUTPUTS_AND_HASHES"] = {"pass": s5_ok}

    overall = all(v.get("pass") is True for v in gates.values())
    report = [
        "# Report - Burners Ranking OEE LP/CP Daily V2 Warmup Buffer",
        "",
        f"- task_id: `{spec['task_id']}`",
        f"- generated_at_utc: `{generated_at}`",
        f"- overall: `{'PASS' if overall else 'FAIL'}`",
        "",
        "## Rule change",
        f"- warmup_buffer_sessions={warmup_buffer}",
        "- baseline_end for preexisting tickers shifted back by buffer sessions inside warmup;",
        "- eligible_from_date for preexisting tickers set to baseline_end + buffer sessions.",
        "",
        "## Gates",
        f"- S1_GATE_ALLOWLIST: `{'PASS' if gates['S1_GATE_ALLOWLIST']['pass'] else 'FAIL'}`",
        f"- S2_CHECK_COMPILE: `{'PASS' if gates['S2_CHECK_COMPILE']['pass'] else 'FAIL'}`",
        f"- S3_RUN: `{'PASS' if gates['S3_RUN']['pass'] else 'FAIL'}`",
        f"- S4_VERIFY_FIRST_SELECTION_NOT_ALL_ZERO: `{'PASS' if gates['S4_VERIFY_FIRST_SELECTION_NOT_ALL_ZERO']['pass'] else 'FAIL'}`",
        f"- S5_VERIFY_OUTPUTS_AND_HASHES: `{'PASS' if gates['S5_VERIFY_OUTPUTS_AND_HASHES']['pass'] else 'FAIL'}`",
    ]
    if not run_ok:
        report += ["", "## Error", f"- {run_error}"]
    write_text(out_root / "report.md", "\n".join(report) + "\n")
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
    hashes[str(out_root / "manifest.json")] = sha256_file(out_root / "manifest.json")
    manifest["hashes_sha256"] = hashes
    write_json(out_root / "manifest.json", manifest)

    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

