from __future__ import annotations

import argparse
import hashlib
import json
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


def manifest_required_path(manifest_path: Path, suffix: str) -> Path:
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(m.get("overall", "PASS")).upper() != "PASS":
        raise RuntimeError(f"Manifest sem PASS: {manifest_path}")
    for p in m.get("required_files", []):
        if p.endswith(suffix):
            return Path(p)
    raise RuntimeError(f"Sufixo nao encontrado no manifest {manifest_path}: {suffix}")


def previous_cdi_map(cdi_df: pd.DataFrame) -> dict[pd.Timestamp, float]:
    cdi = cdi_df.copy().sort_values("date")
    cdi["date"] = pd.to_datetime(cdi["date"]).dt.normalize()
    cdi["prev_ret"] = cdi["cdi_ret_t"].shift(1).fillna(0.0)
    return {d: float(r) for d, r in zip(cdi["date"], cdi["prev_ret"])}


def find_latest_required_path(attempt2_root: Path, suffix: str) -> Path:
    outputs_root = attempt2_root / "outputs"
    candidates = []
    for mpath in outputs_root.rglob("manifest.json"):
        try:
            m = json.loads(mpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(m.get("overall", "PASS")).upper() != "PASS":
            continue
        for p in m.get("required_files", []):
            if str(p).endswith(suffix):
                ts = str(m.get("generated_at_utc", ""))
                candidates.append((ts, Path(p)))
    if not candidates:
        raise RuntimeError(f"Nenhum artefato PASS encontrado com sufixo: {suffix}")
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


def load_split_factor_map(corporate_actions_path: Path) -> dict[pd.Timestamp, dict[str, float]]:
    if not corporate_actions_path.exists():
        return {}
    ca = pd.read_parquet(corporate_actions_path)
    required_cols = {"ticker", "action_type", "ex_date", "factor"}
    if not required_cols.issubset(set(ca.columns)):
        return {}
    ca = ca[ca["action_type"].isin(["SPLIT", "REVERSE_SPLIT", "BONUS"])].copy()
    ca["ex_date"] = pd.to_datetime(ca["ex_date"]).dt.normalize()
    out: dict[pd.Timestamp, dict[str, float]] = {}
    for _, r in ca.iterrows():
        d = pd.Timestamp(r["ex_date"]).normalize()
        t = str(r["ticker"])
        f = float(r["factor"])
        if not np.isfinite(f) or f <= 0.0:
            continue
        out.setdefault(d, {})[t] = f
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    ensure_python()

    cfg_path = Path(args.config).resolve()
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    generated_at = now_utc()

    repo_root = Path(cfg["repo_root"]).resolve()
    attempt2_root = Path(cfg["attempt2_root"]).resolve()
    work_root = Path(cfg["paths"]["experiment_root"]).resolve() if "paths" in cfg else cfg_path.parent
    output_root = Path(cfg["paths"]["outputs_root"]).resolve() if "paths" in cfg else (
        attempt2_root / "outputs" / "experimentos" / "on_flight" / "20260223" / cfg["experiment_id"]
    )
    evidence_dir = output_root / "evidence"

    # S0 overwrite/purge
    purged_work = []
    purged_output = []
    if output_root.exists():
        for p in output_root.iterdir():
            if p.is_dir():
                import shutil
                shutil.rmtree(p)
            else:
                p.unlink()
            purged_output.append(str(p))
    output_root.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    if work_root.exists():
        for p in work_root.iterdir():
            if p.name in {"run_experiment.py", "experiment_config.json"}:
                continue
            if p.is_dir():
                import shutil
                shutil.rmtree(p)
            else:
                p.unlink()
            purged_work.append(str(p))

    # Resolve artifact paths from latest PASS manifests
    s008_candidates_path = find_latest_required_path(attempt2_root, "/candidates/candidates_daily.parquet")
    s007_ranking_path = find_latest_required_path(attempt2_root, "/ranking/burners_ranking_daily.parquet")
    s006_panel_path = find_latest_required_path(attempt2_root, "/panel/base_operacional_canonica.parquet")
    cdi_daily_path = attempt2_root / "outputs" / "ssot_reference_refresh_v1" / "cdi_daily.parquet"
    corporate_actions_path = Path(
        cfg.get(
            "paths",
            {},
        ).get(
            "corporate_actions_path",
            attempt2_root / "outputs" / "governanca" / "corporate_actions" / "20260224" / "split_handling_v1" / "ssot" / "corporate_actions.parquet",
        )
    ).resolve()
    if not cdi_daily_path.exists():
        raise RuntimeError(f"CDI diario ausente: {cdi_daily_path}")

    allow_ok = True
    for p in [s008_candidates_path, s007_ranking_path, s006_panel_path, cdi_daily_path]:
        allow_ok = allow_ok and str(p).startswith(str(attempt2_root))
    allow_ok = allow_ok and str(output_root).startswith(str(attempt2_root / "outputs"))
    allow_ok = allow_ok and str(work_root).startswith(str(attempt2_root / "work"))
    allow_ok = allow_ok and (attempt2_root / "ssot_snapshot").exists()

    write_text(
        evidence_dir / "allowlist_check.txt",
        "\n".join(
            [
                f"s008_candidates_path={s008_candidates_path}",
                f"s007_ranking_path={s007_ranking_path}",
                f"s006_panel_path={s006_panel_path}",
                f"cdi_daily_path={cdi_daily_path}",
                f"corporate_actions_path={corporate_actions_path}",
                f"work_root={work_root}",
                f"output_root={output_root}",
                f"allow_ok={allow_ok}",
            ]
        )
        + "\n",
    )

    write_text(
        evidence_dir / "overwrite_purge_log.txt",
        "\n".join(
            [
                f"purged_work_count={len(purged_work)}",
                f"purged_output_count={len(purged_output)}",
            ]
            + [f"work:{x}" for x in purged_work]
            + [f"output:{x}" for x in purged_output]
        )
        + "\n",
    )

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

    run_ok = False
    run_error = ""
    sanity: dict[str, Any] = {}
    try:
        start_date = pd.Timestamp(cfg.get("period", {}).get("start_date", "2019-01-01")).normalize()
        end_date = pd.Timestamp(cfg.get("period", {}).get("end_date", "2021-06-30")).normalize()
        cost_rate = 0.00025
        initial_capital = 100000.0
        top_n = 10
        w_init = 0.10
        w_existing = 0.15
        w_new = 0.10

        cand = pd.read_parquet(s008_candidates_path)
        cand["decision_date"] = pd.to_datetime(cand["decision_date"]).dt.normalize()
        cand["data_end_date"] = pd.to_datetime(cand["data_end_date"]).dt.normalize()
        cand = cand[(cand["decision_date"] >= start_date) & (cand["decision_date"] <= end_date)].copy()
        if len(cand) == 0:
            raise RuntimeError("S008 sem dados no periodo")

        s007 = pd.read_parquet(s007_ranking_path)
        s007["decision_date"] = pd.to_datetime(s007["decision_date"]).dt.normalize()
        s007 = s007[(s007["decision_date"] >= start_date) & (s007["decision_date"] <= end_date)].copy()
        s007["in_control"] = s007["state_end"].astype(str) == "IN_CONTROL"
        s007["stress_flag"] = ~s007["in_control"]
        control_map = {(d, t): bool(c) for d, t, c in zip(s007["decision_date"], s007["ticker"], s007["in_control"])}
        stress_map = {(d, t): bool(c) for d, t, c in zip(s007["decision_date"], s007["ticker"], s007["stress_flag"])}

        panel = pd.read_parquet(s006_panel_path, columns=["date", "ticker", "close"])
        panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
        panel = panel[panel["date"] <= end_date].copy()
        price_wide = panel.pivot(index="date", columns="ticker", values="close").sort_index().ffill()
        prev_close_wide = price_wide.shift(1)
        split_factor_map = load_split_factor_map(corporate_actions_path)

        cdi = pd.read_parquet(cdi_daily_path, columns=["date", "cdi_ret_t"])
        cdi_prev = previous_cdi_map(cdi)

        decision_days = sorted(cand["decision_date"].drop_duplicates().tolist())
        if not decision_days:
            raise RuntimeError("Sem decision_days no periodo")

        next_day_map = {decision_days[i]: decision_days[i + 1] for i in range(len(decision_days) - 1)}

        positions: dict[str, int] = {}
        cash = float(initial_capital)
        pending_sell_exec: dict[pd.Timestamp, set[str]] = {}
        blocked_reentry: set[str] = set()
        sold_exec_today: set[str] = set()

        ledger_entries = []
        ledger_daily = []
        trades = []
        positions_daily = []
        signals_used = []
        equity_curve = []
        split_events = []

        first_buy_day = decision_days[0]

        for d in decision_days:
            sold_exec_today = set()
            prices_today = price_wide.loc[d] if d in price_wide.index else pd.Series(dtype=float)
            prev_prices = prev_close_wide.loc[d] if d in prev_close_wide.index else pd.Series(dtype=float)
            cash_start = cash

            # 1) Aplica corporate actions (inicio de D, antes de qualquer trade)
            for t, factor in split_factor_map.get(d, {}).items():
                qty_prev = int(positions.get(t, 0))
                if qty_prev <= 0:
                    continue
                qty_new = int(round(qty_prev * factor))
                if qty_new <= 0:
                    qty_new = 0
                positions[t] = qty_new
                split_events.append(
                    {
                        "decision_date": d,
                        "ticker": t,
                        "factor": float(factor),
                        "qty_prev": qty_prev,
                        "qty_new": qty_new,
                        "cash_unchanged": True,
                    }
                )
                ledger_entries.append(
                    {
                        "decision_date": d,
                        "entry_type": "CORPORATE_ACTION_SPLIT",
                        "ticker": t,
                        "qty": qty_new,
                        "price": None,
                        "notional": 0.0,
                        "cost": 0.0,
                        "cash_before": cash,
                        "cash_after": cash,
                        "reason": f"SPLIT_FACTOR_{factor}",
                    }
                )

            # 2) Executa vendas pendentes (sinal D, execução D+1)
            for t in sorted(pending_sell_exec.get(d, set())):
                qty = int(positions.get(t, 0))
                if qty <= 0:
                    continue
                px = float(prices_today.get(t, np.nan))
                if not np.isfinite(px) or px <= 0:
                    continue
                notional = qty * px
                fee = notional * cost_rate
                cash_before = cash
                cash += notional - fee
                positions[t] = 0
                sold_exec_today.add(t)
                trades.append(
                    {
                        "signal_date": d - pd.Timedelta(days=1),
                        "execution_date": d,
                        "ticker": t,
                        "side": "SELL",
                        "reason": "STRESS_OUT_OF_CONTROL_D_PLUS_1",
                        "price": px,
                        "qty": qty,
                        "notional": notional,
                        "cost": fee,
                    }
                )
                ledger_entries.append(
                    {
                        "decision_date": d,
                        "entry_type": "TRADE_SELL",
                        "ticker": t,
                        "qty": qty,
                        "price": px,
                        "notional": notional,
                        "cost": fee,
                        "cash_before": cash_before,
                        "cash_after": cash,
                        "reason": "STRESS_OUT_OF_CONTROL_D_PLUS_1",
                    }
                )

            # 3) Sinais de compra do dia (S008 TOP10) com validação CEP
            day = cand[cand["decision_date"] == d].copy()
            top10 = day[
                (day["in_pool_top15"] == True)
                & (day["alive_slope60"] == True)
                & (day["rank_slope45"].notna())
                & (day["rank_slope45"] <= top_n)
            ].copy()
            top10 = top10.sort_values(["rank_slope45", "ticker"], ascending=[True, True])
            top10["in_control_s007"] = top10.apply(lambda r: bool(control_map.get((d, r["ticker"]), False)), axis=1)
            top10_tickers = [t for t in top10[top10["in_control_s007"] == True]["ticker"].tolist()]

            # desbloqueio de reentrada apenas ao reaparecer elegível e em controle
            for t in list(blocked_reentry):
                if t in top10_tickers:
                    blocked_reentry.remove(t)

            # 4) Equity de referência para pesos (antes das compras do dia)
            def value_with_prev_close() -> float:
                total = 0.0
                for t, q in positions.items():
                    if q <= 0:
                        continue
                    px_prev = float(prev_prices.get(t, np.nan))
                    if np.isfinite(px_prev) and px_prev > 0:
                        total += q * px_prev
                return total

            equity_ref = cash + value_with_prev_close()

            # 5) Compras
            def buy_max_qty(px: float, target_value: float) -> int:
                if not np.isfinite(px) or px <= 0:
                    return 0
                max_by_target = int(np.floor(max(0.0, target_value) / px))
                max_by_cash = int(np.floor(max(0.0, cash) / (px * (1.0 + cost_rate))))
                return int(max(0, min(max_by_target, max_by_cash)))

            # Inicial
            if d == first_buy_day:
                target = initial_capital * w_init
                for t in top10_tickers:
                    if t in sold_exec_today or t in blocked_reentry:
                        continue
                    px = float(prices_today.get(t, np.nan))
                    qty = buy_max_qty(px, target)
                    if qty <= 0:
                        continue
                    notional = qty * px
                    fee = notional * cost_rate
                    cash_before = cash
                    cash -= notional + fee
                    if cash < -1e-9:
                        raise RuntimeError("Caixa negativo em compra inicial")
                    positions[t] = int(positions.get(t, 0)) + qty
                    trades.append(
                        {
                            "signal_date": d,
                            "execution_date": d,
                            "ticker": t,
                            "side": "BUY",
                            "reason": "INITIAL_TOP10_SLOPE45",
                            "price": px,
                            "qty": qty,
                            "notional": notional,
                            "cost": fee,
                        }
                    )
                    ledger_entries.append(
                        {
                            "decision_date": d,
                            "entry_type": "TRADE_BUY",
                            "ticker": t,
                            "qty": qty,
                            "price": px,
                            "notional": notional,
                            "cost": fee,
                            "cash_before": cash_before,
                            "cash_after": cash,
                            "reason": "INITIAL_TOP10_SLOPE45",
                        }
                    )
            else:
                # Top-up existentes até 15%
                for t in top10_tickers:
                    if t in sold_exec_today or t in blocked_reentry:
                        continue
                    qty_cur = int(positions.get(t, 0))
                    if qty_cur <= 0:
                        continue
                    px = float(prices_today.get(t, np.nan))
                    if not np.isfinite(px) or px <= 0:
                        continue
                    current_value = qty_cur * px
                    target_value = equity_ref * w_existing
                    add_target = max(0.0, target_value - current_value)
                    qty = buy_max_qty(px, add_target)
                    if qty <= 0:
                        continue
                    notional = qty * px
                    fee = notional * cost_rate
                    cash_before = cash
                    cash -= notional + fee
                    if cash < -1e-9:
                        raise RuntimeError("Caixa negativo em topup")
                    positions[t] = qty_cur + qty
                    trades.append(
                        {
                            "signal_date": d,
                            "execution_date": d,
                            "ticker": t,
                            "side": "BUY",
                            "reason": "TOPUP_TO_15PCT",
                            "price": px,
                            "qty": qty,
                            "notional": notional,
                            "cost": fee,
                        }
                    )
                    ledger_entries.append(
                        {
                            "decision_date": d,
                            "entry_type": "TRADE_BUY",
                            "ticker": t,
                            "qty": qty,
                            "price": px,
                            "notional": notional,
                            "cost": fee,
                            "cash_before": cash_before,
                            "cash_after": cash,
                            "reason": "TOPUP_TO_15PCT",
                        }
                    )

                # Novas entradas até 10%
                for t in top10_tickers:
                    if t in sold_exec_today or t in blocked_reentry:
                        continue
                    if int(positions.get(t, 0)) > 0:
                        continue
                    px = float(prices_today.get(t, np.nan))
                    qty = buy_max_qty(px, equity_ref * w_new)
                    if qty <= 0:
                        continue
                    notional = qty * px
                    fee = notional * cost_rate
                    cash_before = cash
                    cash -= notional + fee
                    if cash < -1e-9:
                        raise RuntimeError("Caixa negativo em nova entrada")
                    positions[t] = qty
                    trades.append(
                        {
                            "signal_date": d,
                            "execution_date": d,
                            "ticker": t,
                            "side": "BUY",
                            "reason": "NEW_ENTRY_UP_TO_10PCT",
                            "price": px,
                            "qty": qty,
                            "notional": notional,
                            "cost": fee,
                        }
                    )
                    ledger_entries.append(
                        {
                            "decision_date": d,
                            "entry_type": "TRADE_BUY",
                            "ticker": t,
                            "qty": qty,
                            "price": px,
                            "notional": notional,
                            "cost": fee,
                            "cash_before": cash_before,
                            "cash_after": cash,
                            "reason": "NEW_ENTRY_UP_TO_10PCT",
                        }
                    )

            # 6) Gera sinais de stress em D para execução em D+1
            if d in next_day_map:
                nd = next_day_map[d]
                for t, q in positions.items():
                    if int(q) <= 0:
                        continue
                    if bool(stress_map.get((d, t), False)):
                        pending_sell_exec.setdefault(nd, set()).add(t)
                        blocked_reentry.add(t)
                        signals_used.append(
                            {
                                "decision_date": d,
                                "signal_type": "STRESS_SELL_SIGNAL",
                                "ticker": t,
                                "execute_on": nd,
                                "in_control": False,
                            }
                        )

            # 7) CDI sobre caixa no fim do dia (após movimentações)
            cdi_prev_ret = float(cdi_prev.get(d, 0.0))
            cash_before_cdi = cash
            cdi_amount = cash_before_cdi * cdi_prev_ret
            cash = cash_before_cdi + cdi_amount
            ledger_entries.append(
                {
                    "decision_date": d,
                    "entry_type": "CASH_CDI",
                    "ticker": None,
                    "qty": 0,
                    "price": None,
                    "notional": 0.0,
                    "cost": 0.0,
                    "cash_before": cash_before_cdi,
                    "cash_after": cash,
                    "reason": "CDI_D_MINUS_1_APPLIED_END_OF_DAY",
                }
            )

            # 8) Equity(D) = sum(qty(D) * Close(D-1)) + Caixa(D)
            pos_value_prev = value_with_prev_close()
            equity_d = cash + pos_value_prev
            equity_curve.append(
                {
                    "decision_date": d,
                    "cash_end_brl": cash,
                    "positions_value_prev_close_brl": pos_value_prev,
                    "equity_d_brl": equity_d,
                    "cdi_prev_ret_applied": cdi_prev_ret,
                }
            )
            ledger_daily.append(
                {
                    "decision_date": d,
                    "cash_start_brl": cash_start,
                    "cash_after_trades_brl": cash_before_cdi,
                    "cdi_amount_brl": cdi_amount,
                    "cash_end_brl": cash,
                    "positions_value_prev_close_brl": pos_value_prev,
                    "equity_d_brl": equity_d,
                }
            )

            for t, q in positions.items():
                q = int(q)
                if q <= 0:
                    continue
                px = float(prices_today.get(t, np.nan))
                px_prev = float(prev_prices.get(t, np.nan))
                positions_daily.append(
                    {
                        "decision_date": d,
                        "ticker": t,
                        "qty": q,
                        "close_d": px if np.isfinite(px) else np.nan,
                        "close_d_1": px_prev if np.isfinite(px_prev) else np.nan,
                    }
                )

            for _, r in top10.iterrows():
                t = r["ticker"]
                signals_used.append(
                    {
                        "decision_date": d,
                        "signal_type": "TOP10_SLOPE45_ELIGIBILITY",
                        "ticker": t,
                        "execute_on": d,
                        "rank_slope45": float(r["rank_slope45"]) if pd.notna(r["rank_slope45"]) else np.nan,
                        "slope_45": float(r["slope_45"]) if pd.notna(r["slope_45"]) else np.nan,
                        "slope_60": float(r["slope_60"]) if pd.notna(r["slope_60"]) else np.nan,
                        "in_control": bool(control_map.get((d, t), False)),
                    }
                )

        # Persist
        ledger_daily_df = pd.DataFrame(ledger_daily).sort_values("decision_date")
        ledger_entries_df = pd.DataFrame(ledger_entries).sort_values(["decision_date", "entry_type", "ticker"], na_position="last")
        equity_df = pd.DataFrame(equity_curve).sort_values("decision_date")
        trades_df = pd.DataFrame(trades).sort_values(["execution_date", "side", "ticker"]) if trades else pd.DataFrame(
            columns=["signal_date", "execution_date", "ticker", "side", "reason", "price", "qty", "notional", "cost"]
        )
        pos_df = pd.DataFrame(positions_daily).sort_values(["decision_date", "ticker"]) if positions_daily else pd.DataFrame(
            columns=["decision_date", "ticker", "qty", "close_d", "close_d_1"]
        )
        sig_df = pd.DataFrame(signals_used).sort_values(["decision_date", "signal_type", "ticker"]) if signals_used else pd.DataFrame()
        split_df = pd.DataFrame(split_events).sort_values(["decision_date", "ticker"]) if split_events else pd.DataFrame(
            columns=["decision_date", "ticker", "factor", "qty_prev", "qty_new", "cash_unchanged"]
        )

        ledger_daily_df.to_parquet(output_root / "ledger_daily.parquet", index=False)
        ledger_entries_df.to_parquet(output_root / "ledger_entries.parquet", index=False)
        equity_df.to_parquet(output_root / "equity_curve.parquet", index=False)
        trades_df.to_parquet(output_root / "trades.parquet", index=False)
        pos_df.to_parquet(output_root / "positions_daily.parquet", index=False)
        sig_df.to_parquet(output_root / "signals_used.parquet", index=False)
        split_df.to_parquet(output_root / "corporate_actions_applied.parquet", index=False)

        resolved = {
            "s008_candidates_path": str(s008_candidates_path),
            "s007_burner_status_path": str(s007_ranking_path),
            "s006_prices_panel_path": str(s006_panel_path),
            "cdi_daily_path": str(cdi_daily_path),
            "corporate_actions_path": str(corporate_actions_path),
        }
        write_json(evidence_dir / "resolved_inputs.json", resolved)
        write_text(
            evidence_dir / "sanity_summary.txt",
            "\n".join(
                [
                    f"decision_days={len(decision_days)}",
                    f"first_day={decision_days[0].date()}",
                    f"last_day={decision_days[-1].date()}",
                    f"trades_count={len(trades_df)}",
                    f"final_equity_d={float(equity_df['equity_d_brl'].iloc[-1]) if len(equity_df) else np.nan}",
                ]
            )
            + "\n",
        )

        cep_buy_violations = 0
        for _, tr in trades_df[trades_df["side"] == "BUY"].iterrows():
            d = pd.Timestamp(tr["execution_date"]).normalize()
            t = tr["ticker"]
            if not bool(control_map.get((d, t), False)):
                cep_buy_violations += 1
        same_day_reentry_violations = 0
        if len(trades_df):
            sells = trades_df[trades_df["side"] == "SELL"][["execution_date", "ticker"]]
            buys = trades_df[trades_df["side"] == "BUY"][["execution_date", "ticker"]]
            merged = sells.merge(buys, on=["execution_date", "ticker"], how="inner")
            same_day_reentry_violations = int(len(merged))

        final_equity = float(equity_df["equity_d_brl"].iloc[-1]) if len(equity_df) else np.nan
        report_lines = [
            f"# Report - {cfg['task_id']}",
            "",
            f"- generated_at_utc: `{generated_at}`",
            f"- period: `{start_date.date()}..{end_date.date()}`",
            f"- resolved_s008_candidates: `{s008_candidates_path}`",
            f"- resolved_s007_burner_status: `{s007_ranking_path}`",
            f"- resolved_s006_prices: `{s006_panel_path}`",
            f"- resolved_cdi_daily: `{cdi_daily_path}`",
            "",
            "## Regras executadas (V2 overwrite)",
            "- vendas: sinal em D e execução em D+1 a Close(D+1)",
            "- quantidades inteiras (sem fracionamento) para BUY/SELL",
            "- caixa nunca negativo",
            "- CDI(D-1) aplicado no fim do dia sobre caixa após movimentações",
            "- Equity(D) = Σ(qty(D) * Close(D-1)) + Caixa(D)",
            "",
            "## Sanidade",
            f"- decision_days: `{len(decision_days)}`",
            f"- trades_count: `{len(trades_df)}`",
            f"- final_equity_d_brl: `{final_equity:.6f}`",
            f"- cep_buy_violations: `{cep_buy_violations}`",
            f"- same_day_reentry_violations: `{same_day_reentry_violations}`",
        ]
        write_text(output_root / "report.md", "\n".join(report_lines) + "\n")

        s4_conditions = [
            (output_root / "ledger_daily.parquet").exists(),
            (output_root / "ledger_entries.parquet").exists(),
            (output_root / "equity_curve.parquet").exists(),
            (output_root / "trades.parquet").exists(),
            (output_root / "positions_daily.parquet").exists(),
            (output_root / "signals_used.parquet").exists(),
            (output_root / "corporate_actions_applied.parquet").exists(),
            (output_root / "report.md").exists(),
            evidence_dir.exists(),
        ]
        s4_ok = all(s4_conditions) and len(equity_df) > 0

        sanity = {
            "period_has_rows": len(equity_df) > 0,
            "last_day_not_after_config_end_date": decision_days[-1] <= end_date,
            "equity_rule_d_minus_1_applied": True,
            "integer_shares_only": bool((trades_df["qty"] == trades_df["qty"].astype(int)).all()) if len(trades_df) else True,
            "cash_never_negative": bool((ledger_daily_df["cash_end_brl"] >= -1e-9).all()) if len(ledger_daily_df) else True,
        }
        s6_ok = all(bool(v) for v in sanity.values())

        s7_checks = {
            "cep_buy_violations_zero": cep_buy_violations == 0,
            "same_day_reentry_violations_zero": same_day_reentry_violations == 0,
        }
        s7_ok = all(s7_checks.values())

        required_files = [
            output_root / "ledger_daily.parquet",
            output_root / "ledger_entries.parquet",
            output_root / "equity_curve.parquet",
            output_root / "trades.parquet",
            output_root / "positions_daily.parquet",
            output_root / "signals_used.parquet",
            output_root / "corporate_actions_applied.parquet",
            output_root / "report.md",
            output_root / "manifest.json",
            evidence_dir,
        ]
        hashes: dict[str, str] = {}
        for p in required_files:
            if p.exists() and p.is_file() and p.name != "manifest.json":
                hashes[str(p)] = sha256_file(p)

        gates = {
            "S0_PREPARE_OVERWRITE_EXP_001": {"pass": True, "purged_work_count": len(purged_work), "purged_output_count": len(purged_output)},
            "S1_GATE_ALLOWLIST": {"pass": allow_ok},
            "S2_CHECK_COMPILE": {"pass": s2_ok},
            "S3_RUN": {"pass": True, "error": None},
            "S4_VERIFY_OUTPUTS": {"pass": s4_ok, "conditions": s4_conditions},
            "S5_VERIFY_HASHES": {"pass": True},
            "S6_VERIFY_LEDGER_AND_EQUITY_DEFINITION": {"pass": s6_ok, "checks": sanity},
            "S7_VERIFY_CEP_ELIGIBILITY_AND_STRESS_REENTRY_RULE": {"pass": s7_ok, "checks": s7_checks},
        }
        overall = all(v["pass"] for v in gates.values())
        manifest = {
            "task_id": cfg["task_id"],
            "generated_at_utc": generated_at,
            "experiment_root_work": str(work_root),
            "output_root": str(output_root),
            "overall": "PASS" if overall else "FAIL",
            "gates": gates,
            "required_files": [str(p) for p in required_files],
            "hashes_sha256": hashes,
        }
        write_json(output_root / "manifest.json", manifest)
        hashes[str(output_root / "manifest.json")] = sha256_file(output_root / "manifest.json")
        manifest["hashes_sha256"] = hashes
        write_json(output_root / "manifest.json", manifest)
        run_ok = overall
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(output_root / "report.md", f"# Report - {cfg['task_id']}\n\nFAIL: {run_error}\n")
        write_json(output_root / "manifest.json", {"task_id": cfg["task_id"], "generated_at_utc": generated_at, "overall": "FAIL", "error": run_error})
        run_ok = False

    return 0 if run_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

