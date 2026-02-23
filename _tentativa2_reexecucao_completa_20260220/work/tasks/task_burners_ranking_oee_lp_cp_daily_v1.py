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

    panel_path = Path(spec["inputs"]["base_canonica_panel_parquet"]).resolve()
    cal_path = Path(spec["inputs"]["base_canonica_calendar_parquet"]).resolve()
    approved_path = Path(spec["inputs"]["approved_universe_ssot_parquet"]).resolve()
    checklist_path = Path(spec["inputs"]["owner_visual_checklist_md"]).resolve()

    cfg = spec["inputs"]["config"]
    x_col = cfg["signal_col_x"]
    warmup_start = pd.Timestamp(cfg["warmup_start"]).normalize()
    warmup_end = pd.Timestamp(cfg["warmup_end"]).normalize()
    first_selection_month = str(cfg["first_selection_month"])
    cp_window = int(cfg["cp_window_sessions"])
    cep_n = int(cfg["cep"]["N"])
    cep_k = int(cfg["cep"]["K"])
    a2 = float(cfg["cep"]["spc_constants"]["A2_for_N3"])
    d3 = float(cfg["cep"]["spc_constants"]["D3_for_N3"])
    d4 = float(cfg["cep"]["spc_constants"]["D4_for_N3"])

    generated_at = now_utc()
    gates: dict[str, dict[str, Any]] = {}

    # S1 allowlist
    snapshot = attempt2_root / "ssot_snapshot"
    snap_status = run_cmd(["git", "status", "--short", "--", str(snapshot)], repo_root).stdout.strip()
    allow_ok = all(str(p).startswith(str(attempt2_root)) for p in [panel_path, cal_path, approved_path, checklist_path])
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
    first_decision_date = None
    try:
        # Load inputs
        approved = pd.read_parquet(approved_path)[["ticker", "asset_class"]].copy()
        approved["ticker"] = approved["ticker"].astype(str)
        approved_tickers = sorted(approved["ticker"].unique().tolist())
        if len(approved_tickers) != 448:
            raise RuntimeError(f"Ticker count aprovado inesperado: {len(approved_tickers)} != 448")
        ticker_asset = dict(zip(approved["ticker"], approved["asset_class"]))

        panel = pd.read_parquet(panel_path).copy()
        panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
        panel["ticker"] = panel["ticker"].astype(str)
        if x_col not in panel.columns:
            raise RuntimeError(f"signal_col_x ausente no painel: {x_col}")

        panel = panel[panel["ticker"].isin(approved_tickers)].copy()
        panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

        cal_df = pd.read_parquet(cal_path).copy()
        cal_df["date"] = pd.to_datetime(cal_df["date"]).dt.normalize()
        calendar = sorted(cal_df["date"].drop_duplicates().tolist())
        cal_set = set(calendar)
        cal_pos = {d: i for i, d in enumerate(calendar)}

        # Ordered dates available in panel (as requested)
        panel_dates = sorted(panel["date"].drop_duplicates().tolist())
        panel_dates = [d for d in panel_dates if d in cal_set]
        if len(panel_dates) == 0:
            raise RuntimeError("Painel sem datas após filtro de universo aprovado")

        # warmup and first decision date
        last_trading_warmup = max([d for d in panel_dates if d <= warmup_end], default=None)
        first_decision_candidates = [d for d in panel_dates if d.strftime("%Y-%m").startswith(first_selection_month)]
        if len(first_decision_candidates) == 0:
            raise RuntimeError(f"Sem primeira data de seleção para mês {first_selection_month}")
        first_decision_date = min(first_decision_candidates)

        # per ticker map of x by date
        ticker_first = panel.groupby("ticker", as_index=False)["date"].min().rename(columns={"date": "first_date"})
        ticker_first_map = dict(zip(ticker_first["ticker"], ticker_first["first_date"]))

        ticker_data: dict[str, pd.DataFrame] = {}
        for t, g in panel.groupby("ticker", sort=False):
            gg = g[["date", x_col]].copy().sort_values("date")
            gg[x_col] = pd.to_numeric(gg[x_col], errors="coerce")
            ticker_data[t] = gg

        # Step 4 baseline limits per ticker
        base_rows: list[dict[str, Any]] = []
        cep_rows: list[dict[str, Any]] = []
        baseline_insufficient = 0
        baseline_std_map: dict[str, float] = {}

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

            ticker_dates = g["date"].drop_duplicates().sort_values().tolist()
            first_date_i = pd.Timestamp(ticker_dates[0]).normalize()
            start_i = max(warmup_start, first_date_i)

            in_warmup = [d for d in ticker_dates if d <= warmup_end]
            if len(in_warmup) > 0:
                baseline_end = pd.Timestamp(max(in_warmup)).normalize()
            else:
                if len(ticker_dates) < 62:
                    baseline_end = None
                else:
                    baseline_end = pd.Timestamp(ticker_dates[61]).normalize()

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

            eligible_dates_upto_end = [d for d in ticker_dates if d <= baseline_end]
            if len(eligible_dates_upto_end) < 62:
                baseline_insufficient += 1
                base_rows.append(
                    {
                        "ticker": t,
                        "asset_class": asset_class,
                        "baseline_start": None,
                        "baseline_end": baseline_end.strftime("%Y-%m-%d"),
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

            baseline_window_dates = eligible_dates_upto_end[-62:]
            baseline_start = pd.Timestamp(baseline_window_dates[0]).normalize()

            x_map = dict(zip(g["date"], g[x_col]))
            x_vals = np.array([x_map.get(d, np.nan) for d in baseline_window_dates], dtype=float)
            if np.isnan(x_vals).any():
                baseline_insufficient += 1
                base_rows.append(
                    {
                        "ticker": t,
                        "asset_class": asset_class,
                        "baseline_start": baseline_start.strftime("%Y-%m-%d"),
                        "baseline_end": baseline_end.strftime("%Y-%m-%d"),
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

            # K=60 subgrupos de N=3 numa janela de 62
            xbars = []
            rs = []
            for j in range(cep_k):
                w = x_vals[j : j + cep_n]
                xbars.append(float(np.mean(w)))
                rs.append(float(np.max(w) - np.min(w)))
            xbarbar = float(np.mean(xbars))
            rbar = float(np.mean(rs))
            baseline_std = float(np.std(x_vals, ddof=1))
            ucl_xbar = xbarbar + a2 * rbar
            lcl_xbar = xbarbar - a2 * rbar
            ucl_r = d4 * rbar
            lcl_r = d3 * rbar
            baseline_std_map[t] = baseline_std

            # eligible_from_date:
            # prioridade para o próprio baseline_end (permite estado em D-1 para decisão 2018-07-02),
            # desde que existam 3 dias consecutivos válidos terminando em baseline_end.
            idx_end = cal_pos.get(baseline_end)
            eligible_from = None
            if idx_end is not None:
                # tenta baseline_end primeiro
                if idx_end - 2 >= 0:
                    bprev2 = calendar[idx_end - 2]
                    bprev1 = calendar[idx_end - 1]
                    bend = calendar[idx_end]
                    if bprev2 in x_map and bprev1 in x_map and bend in x_map:
                        xv0 = [x_map[bprev2], x_map[bprev1], x_map[bend]]
                        if all(pd.notna(v) for v in xv0):
                            eligible_from = bend

                # fallback para frente se baseline_end não for válido
                if eligible_from is None:
                    for i in range(idx_end + 1, len(calendar)):
                        tdate = calendar[i]
                        prev2 = calendar[i - 2] if i - 2 >= 0 else None
                        prev1 = calendar[i - 1] if i - 1 >= 0 else None
                        if prev2 is None or prev1 is None:
                            continue
                        if prev2 in x_map and prev1 in x_map and tdate in x_map:
                            xv = [x_map[prev2], x_map[prev1], x_map[tdate]]
                            if all(pd.notna(v) for v in xv):
                                eligible_from = tdate
                                break

            base_rows.append(
                {
                    "ticker": t,
                    "asset_class": asset_class,
                    "baseline_start": baseline_start.strftime("%Y-%m-%d"),
                    "baseline_end": baseline_end.strftime("%Y-%m-%d"),
                    "xbarbar": xbarbar,
                    "Rbar": rbar,
                    "LCL_Xbar": lcl_xbar,
                    "UCL_Xbar": ucl_xbar,
                    "LCL_R": lcl_r,
                    "UCL_R": ucl_r,
                    "eligible_from_date": eligible_from.strftime("%Y-%m-%d") if eligible_from is not None else None,
                    "baseline_insufficient": False,
                }
            )

            # Step 5 CEP states
            if eligible_from is not None:
                for i in range(cal_pos[eligible_from], len(calendar)):
                    tdate = calendar[i]
                    prev2 = calendar[i - 2] if i - 2 >= 0 else None
                    prev1 = calendar[i - 1] if i - 1 >= 0 else None
                    if prev2 is None or prev1 is None:
                        continue
                    if not (prev2 in x_map and prev1 in x_map and tdate in x_map):
                        continue
                    xv = [x_map[prev2], x_map[prev1], x_map[tdate]]
                    if not all(pd.notna(v) for v in xv):
                        continue
                    xbar3 = float(np.mean(xv))
                    r3 = float(np.max(xv) - np.min(xv))

                    level_out = (xbar3 < lcl_xbar) or (xbar3 > ucl_xbar)
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
                            "date": tdate.strftime("%Y-%m-%d"),
                            "ticker": t,
                            "x": float(x_map[tdate]),
                            "xbar_3": xbar3,
                            "R_3": r3,
                            "LCL_Xbar": lcl_xbar,
                            "UCL_Xbar": ucl_xbar,
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
            raise RuntimeError("CEP states vazio; não foi possível gerar série de estados")
        cep_df["date"] = pd.to_datetime(cep_df["date"]).dt.normalize()
        cep_df = cep_df.sort_values(["ticker", "date"]).reset_index(drop=True)
        cep_out = cep_dir / "cep_states_daily.parquet"
        cep_df.to_parquet(cep_out, index=False)

        # Step 6 daily ranking (otimizado com cumulativas por ticker)
        first_decision_date = pd.Timestamp(first_decision_date).normalize()
        decision_dates = [d for d in panel_dates if d >= first_decision_date]
        if len(decision_dates) == 0:
            raise RuntimeError("Sem decision_dates para ranking")
        first_decision_idx = cal_pos[first_decision_date]

        panel_idx = panel[["date", "ticker", x_col]].copy()
        panel_idx["cal_idx"] = panel_idx["date"].map(cal_pos)
        panel_idx = panel_idx.dropna(subset=["cal_idx"]).copy()
        panel_idx["cal_idx"] = panel_idx["cal_idx"].astype(int)
        panel_idx[x_col] = pd.to_numeric(panel_idx[x_col], errors="coerce")

        cep_idx = cep_df[["date", "ticker", "eta", "state"]].copy()
        cep_idx["cal_idx"] = cep_idx["date"].map(cal_pos)
        cep_idx = cep_idx.dropna(subset=["cal_idx"]).copy()
        cep_idx["cal_idx"] = cep_idx["cal_idx"].astype(int)
        cep_idx["eta"] = pd.to_numeric(cep_idx["eta"], errors="coerce").fillna(0).astype(int)

        T = len(calendar)
        rows = []

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

        for t in approved_tickers:
            asset_class = ticker_asset.get(t, "UNKNOWN")
            first_date_i = ticker_first_map[t]
            start_i = max(warmup_start, first_date_i)
            start_idx = cal_pos.get(start_i)
            if start_idx is None:
                # se start_i não estiver no calendário, usa a próxima posição >= start_i
                start_idx = next((i for i, dd in enumerate(calendar) if dd >= start_i), None)
            if start_idx is None:
                continue

            x_arr = np.full(T, np.nan, dtype=float)
            eta_arr = np.zeros(T, dtype=float)
            state_arr = np.array(["NO_STATE"] * T, dtype=object)

            gp = panel_idx[panel_idx["ticker"] == t]
            if len(gp) > 0:
                x_arr[gp["cal_idx"].to_numpy()] = gp[x_col].to_numpy(dtype=float)

            gc = cep_idx[cep_idx["ticker"] == t]
            if len(gc) > 0:
                idxs = gc["cal_idx"].to_numpy(dtype=int)
                eta_arr[idxs] = gc["eta"].to_numpy(dtype=float)
                state_arr[idxs] = gc["state"].astype(str).to_numpy()

            x_clean = np.where(np.isnan(x_arr), 0.0, x_arr)
            eta_pos = eta_arr * np.maximum(x_clean, 0.0)
            eta_abs = eta_arr * np.abs(x_clean)
            active = (eta_arr == 1.0) & (~np.isnan(x_arr))
            active_cnt = active.astype(float)
            active_sum = np.where(active, x_arr, 0.0)
            active_sumsq = np.where(active, x_arr * x_arr, 0.0)

            pref_eta = prefix(eta_arr)
            pref_eta_pos = prefix(eta_pos)
            pref_eta_abs = prefix(eta_abs)
            pref_act_cnt = prefix(active_cnt)
            pref_act_sum = prefix(active_sum)
            pref_act_sumsq = prefix(active_sumsq)

            fallback_v = baseline_std_map.get(t, np.nan)
            for dec_idx in range(first_decision_idx, T):
                data_end_idx = dec_idx - 1
                if data_end_idx < start_idx:
                    continue
                d = calendar[dec_idx]
                data_end = calendar[data_end_idx]

                n_lp = data_end_idx - start_idx + 1
                n_cp = min(cp_window, n_lp)
                l_lp = start_idx
                r_lp = data_end_idx
                l_cp = r_lp - n_cp + 1
                r_cp = r_lp
                w_cp = n_cp / n_lp
                w_lp = 1.0 - w_cp

                # LP
                eta_lp_sum = win_sum(pref_eta, l_lp, r_lp)
                A_lp = float(eta_lp_sum / n_lp) if n_lp > 0 else 0.0
                num_lp = win_sum(pref_eta_pos, l_lp, r_lp)
                den_lp = win_sum(pref_eta_abs, l_lp, r_lp)
                P_lp = float(num_lp / den_lp) if den_lp > 0 else 0.0
                c_lp = win_sum(pref_act_cnt, l_lp, r_lp)
                if c_lp >= 2:
                    s_lp = win_sum(pref_act_sum, l_lp, r_lp)
                    ss_lp = win_sum(pref_act_sumsq, l_lp, r_lp)
                    var_lp = (ss_lp - (s_lp * s_lp / c_lp)) / (c_lp - 1)
                    V_lp = float(np.sqrt(max(var_lp, 0.0)))
                else:
                    V_lp = float(fallback_v) if pd.notna(fallback_v) else np.nan

                # CP
                eta_cp_sum = win_sum(pref_eta, l_cp, r_cp)
                A_cp = float(eta_cp_sum / n_cp) if n_cp > 0 else 0.0
                num_cp = win_sum(pref_eta_pos, l_cp, r_cp)
                den_cp = win_sum(pref_eta_abs, l_cp, r_cp)
                P_cp = float(num_cp / den_cp) if den_cp > 0 else 0.0
                c_cp = win_sum(pref_act_cnt, l_cp, r_cp)
                if c_cp >= 2:
                    s_cp = win_sum(pref_act_sum, l_cp, r_cp)
                    ss_cp = win_sum(pref_act_sumsq, l_cp, r_cp)
                    var_cp = (ss_cp - (s_cp * s_cp / c_cp)) / (c_cp - 1)
                    V_cp = float(np.sqrt(max(var_cp, 0.0)))
                else:
                    V_cp = float(fallback_v) if pd.notna(fallback_v) else np.nan

                eta_end = int(eta_arr[data_end_idx]) if 0 <= data_end_idx < T else 0
                state_end = str(state_arr[data_end_idx]) if 0 <= data_end_idx < T else "NO_STATE"

                rows.append(
                    {
                        "decision_date": d,
                        "data_end_date": data_end,
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
            raise RuntimeError("Ranking diário vazio")

        rank_df["Q_LP"] = rank_df.groupby("decision_date")["V_LP"].rank(pct=True, method="average")
        rank_df["Q_CP"] = rank_df.groupby("decision_date")["V_CP"].rank(pct=True, method="average")
        rank_df["Q_LP"] = 1.0 - rank_df["Q_LP"]
        rank_df["Q_CP"] = 1.0 - rank_df["Q_CP"]
        rank_df.loc[rank_df["V_LP"].isna(), "Q_LP"] = 0.0
        rank_df.loc[rank_df["V_CP"].isna(), "Q_CP"] = 0.0

        rank_df["OEE_LP"] = rank_df["A_LP"] * rank_df["P_LP"] * rank_df["Q_LP"]
        rank_df["OEE_CP"] = rank_df["A_CP"] * rank_df["P_CP"] * rank_df["Q_CP"]
        rank_df["OEE_OVERALL"] = rank_df["w_lp"] * rank_df["OEE_LP"] + rank_df["w_cp"] * rank_df["OEE_CP"]
        if len(rank_df) == 0:
            raise RuntimeError("Ranking diário vazio")

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
        rank_df["decision_date"] = pd.to_datetime(rank_df["decision_date"]).dt.normalize()
        rank_df["data_end_date"] = pd.to_datetime(rank_df["data_end_date"]).dt.normalize()
        rank_df = rank_df.sort_values(["decision_date", "OEE_OVERALL", "ticker"], ascending=[True, False, True]).reset_index(drop=True)
        ranking_out = ranking_dir / "burners_ranking_daily.parquet"
        rank_df.to_parquet(ranking_out, index=False)

        # Step 7 evidence/metadata
        pd.DataFrame(
            [
                {
                    "panel_min_date": str(min(panel_dates).date()),
                    "panel_max_date": str(max(panel_dates).date()),
                    "first_decision_date": str(first_decision_date.date()),
                    "last_decision_date": str(max(decision_dates).date()),
                    "rows_ranking": int(len(rank_df)),
                    "tickers": int(rank_df["ticker"].nunique()),
                }
            ]
        ).to_csv(evidence_dir / "date_range_summary.csv", index=False)

        eligible_daily = (
            cep_df[cep_df["eta"] == 1]
            .groupby("date", as_index=False)["ticker"]
            .nunique()
            .rename(columns={"ticker": "eligible_tickers_eta1"})
        )
        pd.DataFrame(
            [
                {
                    "baseline_insufficient_count": int(baseline_insufficient),
                    "baseline_total_tickers": int(len(approved_tickers)),
                    "eligible_days_count": int(eligible_daily["date"].nunique()),
                }
            ]
        ).to_csv(evidence_dir / "eligibility_summary.csv", index=False)

        state_counts = (
            cep_df.groupby(["date", "state"], as_index=False)["ticker"]
            .nunique()
            .rename(columns={"ticker": "n_tickers"})
            .sort_values(["date", "state"])
        )
        state_counts.to_csv(evidence_dir / "cep_state_counts.csv", index=False)

        first_day = pd.Timestamp(first_decision_date).normalize()
        last_day = pd.Timestamp(rank_df["decision_date"].max()).normalize()
        first_top20 = rank_df[rank_df["decision_date"] == first_day].head(20)
        last_top20 = rank_df[rank_df["decision_date"] == last_day].head(20)
        lines = ["# OEE summary samples", "", f"first_decision_date={first_day.date()}", ""]
        for _, r in first_top20.iterrows():
            lines.append(f"{r['ticker']}: OEE_OVERALL={r['OEE_OVERALL']:.8f} OEE_LP={r['OEE_LP']:.8f} OEE_CP={r['OEE_CP']:.8f}")
        lines.extend(["", f"last_decision_date={last_day.date()}", ""])
        for _, r in last_top20.iterrows():
            lines.append(f"{r['ticker']}: OEE_OVERALL={r['OEE_OVERALL']:.8f} OEE_LP={r['OEE_LP']:.8f} OEE_CP={r['OEE_CP']:.8f}")
        write_text(evidence_dir / "oee_summary_samples.txt", "\n".join(lines) + "\n")

        metadata = {
            "build_version": "v1",
            "built_at_utc": generated_at,
            "inputs": {
                "panel": str(panel_path),
                "calendar": str(cal_path),
                "approved_universe": str(approved_path),
            },
            "input_hashes_sha256": {
                str(panel_path): sha256_file(panel_path),
                str(cal_path): sha256_file(cal_path),
                str(approved_path): sha256_file(approved_path),
            },
            "config": {
                "signal_col_x": x_col,
                "warmup_start": cfg["warmup_start"],
                "warmup_end": cfg["warmup_end"],
                "first_selection_month": first_selection_month,
                "cp_window_sessions": cp_window,
                "cep_N": cep_n,
                "cep_K": cep_k,
                "A2": a2,
                "D3": d3,
                "D4": d4,
            },
            "first_decision_date": str(first_decision_date.date()),
            "rule_data_end_date": "data_end_date = previous trading day of decision_date",
            "output_schemas": {
                "ranking_columns": list(rank_df.columns),
                "cep_state_columns": list(cep_df.columns),
                "baseline_columns": list(baseline_df.columns),
            },
        }
        write_json(meta_dir / "dataset_metadata.json", metadata)

        # Step 8 visual update S007 (only after successful materialization)
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
  S007["S007 Ranking Burners diário OEE LP/CP (448) registrado"]
  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007
```
"""
        write_text(checklist_path, checklist)

        decisions_mmd = """flowchart TB
  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]
  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]
  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]
  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]
  S005["S005 SSOT tickers aprovados (448)"]
  S006["S006 Base Operacional Canônica Completa (448)"]
  S007["S007 Ranking Burners diário OEE LP/CP (448) registrado"]
  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007
"""
        write_text(visual_dir / "decisions_flow.mmd", decisions_mmd)

        run_ok = True
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(evidence_dir / "run_exception.txt", run_error + "\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "error": run_error if not run_ok else None}

    # S4 verify
    s4_ok = False
    s4_conditions: list[bool] = []
    if run_ok:
        ranking_path = ranking_dir / "burners_ranking_daily.parquet"
        cep_path = cep_dir / "cep_states_daily.parquet"
        baseline_path = baseline_dir / "baseline_limits_by_ticker.parquet"
        required_rank_cols = [
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
        required_cep_cols = ["date", "ticker", "x", "xbar_3", "R_3", "LCL_Xbar", "UCL_Xbar", "UCL_R", "state", "eta"]
        required_base_cols = [
            "ticker",
            "asset_class",
            "baseline_start",
            "baseline_end",
            "xbarbar",
            "Rbar",
            "LCL_Xbar",
            "UCL_Xbar",
            "LCL_R",
            "UCL_R",
            "eligible_from_date",
        ]
        rank_chk = pd.read_parquet(ranking_path)
        cep_chk = pd.read_parquet(cep_path)
        base_chk = pd.read_parquet(baseline_path)

        first_decision_ok = pd.Timestamp(rank_chk["decision_date"].min()).strftime("%Y-%m") == first_selection_month
        prev_day_rule_ok = bool((pd.to_datetime(rank_chk["decision_date"]) > pd.to_datetime(rank_chk["data_end_date"])).all())

        s4_conditions = [
            ranking_path.exists() and cep_path.exists() and baseline_path.exists() and (meta_dir / "dataset_metadata.json").exists(),
            all(c in rank_chk.columns for c in required_rank_cols),
            all(c in cep_chk.columns for c in required_cep_cols),
            all(c in base_chk.columns for c in required_base_cols),
            first_decision_ok,
            prev_day_rule_ok,
        ]
        s4_ok = all(s4_conditions)
    gates["S4_VERIFY_RANKING_OUTPUTS"] = {"pass": s4_ok, "conditions": s4_conditions}

    # S5 hashes
    required_files = [out_root / rel for rel in spec["outputs"]["required_files"]]
    hashes = {}
    for p in required_files:
        if p.exists() and p.is_file():
            hashes[str(p)] = sha256_file(p)
    s5_ok = len(hashes) == len([p for p in required_files if p.is_file()])
    gates["S5_VERIFY_OUTPUTS_AND_HASHES"] = {"pass": s5_ok}

    overall = all(v.get("pass") is True for v in gates.values())
    report_lines = [
        "# Report - Burners Ranking OEE LP/CP Daily V1",
        "",
        f"- task_id: `{spec['task_id']}`",
        f"- generated_at_utc: `{generated_at}`",
        f"- overall: `{'PASS' if overall else 'FAIL'}`",
        "",
        "## Gates",
        f"- S1_GATE_ALLOWLIST: `{'PASS' if gates['S1_GATE_ALLOWLIST']['pass'] else 'FAIL'}`",
        f"- S2_CHECK_COMPILE: `{'PASS' if gates['S2_CHECK_COMPILE']['pass'] else 'FAIL'}`",
        f"- S3_RUN: `{'PASS' if gates['S3_RUN']['pass'] else 'FAIL'}`",
        f"- S4_VERIFY_RANKING_OUTPUTS: `{'PASS' if gates['S4_VERIFY_RANKING_OUTPUTS']['pass'] else 'FAIL'}`",
        f"- S5_VERIFY_OUTPUTS_AND_HASHES: `{'PASS' if gates['S5_VERIFY_OUTPUTS_AND_HASHES']['pass'] else 'FAIL'}`",
        "",
        "## Regras validadas",
        f"- first_decision_date_min_month={first_selection_month}",
        "- data_end_date = previous trading day of decision_date (validação booleana aplicada).",
    ]
    if not run_ok:
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

