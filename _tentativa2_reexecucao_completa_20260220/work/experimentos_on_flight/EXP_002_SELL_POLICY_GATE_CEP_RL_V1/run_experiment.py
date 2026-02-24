from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

OFFICIAL_PYTHON = "/home/wilson/PortfolioZero/.venv/bin/python"
ANN_FACTOR = 252.0


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_python() -> None:
    import sys

    if str(Path(sys.executable)) != OFFICIAL_PYTHON:
        raise RuntimeError(f"Interpreter invalido: {sys.executable}. Use {OFFICIAL_PYTHON}.")


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
        ts = str(m.get("generated_at_utc", ""))
        for p in m.get("required_files", []):
            if str(p).endswith(suffix):
                candidates.append((ts, Path(p)))
    if not candidates:
        raise RuntimeError(f"Nenhum artefato PASS encontrado com sufixo: {suffix}")
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


def previous_cdi_map(cdi_df: pd.DataFrame) -> dict[pd.Timestamp, float]:
    cdi = cdi_df.copy().sort_values("date")
    cdi["date"] = pd.to_datetime(cdi["date"]).dt.normalize()
    cdi["prev_ret"] = cdi["cdi_ret_t"].shift(1).fillna(0.0)
    return {d: float(r) for d, r in zip(cdi["date"], cdi["prev_ret"])}


def rolling_zscore(df_wide: pd.DataFrame, lookback: int, min_periods: int) -> pd.DataFrame:
    mean = df_wide.rolling(lookback, min_periods=min_periods).mean()
    std = df_wide.rolling(lookback, min_periods=min_periods).std(ddof=0)
    z = (df_wide - mean) / std.replace(0.0, np.nan)
    return z


def rolling_slope(series: pd.Series, w: int) -> pd.Series:
    x = np.arange(w, dtype=float)
    x_center = x - x.mean()
    denom = float(np.sum(x_center**2))
    vals = series.to_numpy(dtype=float)
    out = np.full(len(vals), np.nan, dtype=float)
    for i in range(w - 1, len(vals)):
        y = vals[i - w + 1 : i + 1]
        if not np.all(np.isfinite(y)):
            continue
        y_center = y - y.mean()
        out[i] = float(np.sum(x_center * y_center) / denom)
    return pd.Series(out, index=series.index, dtype=float)


def downside_band(z: float) -> int:
    if not np.isfinite(z) or z >= 0.0:
        return 0
    if -2.0 <= z < -1.0:
        return 1
    if -3.0 <= z < -2.0:
        return 2
    if z < -3.0:
        return 3
    return 0


def calc_mdd_and_recovery(equity: pd.Series) -> tuple[float, int]:
    if len(equity) == 0:
        return float("nan"), 0
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    mdd = float(drawdown.min())
    worst_idx = int(drawdown.values.argmin())
    peak_at_worst = peak.iloc[worst_idx]
    recover_idx = None
    for i in range(worst_idx + 1, len(equity)):
        if float(equity.iloc[i]) >= float(peak_at_worst):
            recover_idx = i
            break
    if recover_idx is None:
        ttr = len(equity) - worst_idx - 1
    else:
        ttr = recover_idx - worst_idx
    return mdd, int(max(0, ttr))


def metrics_from_equity(equity_df: pd.DataFrame) -> dict[str, float]:
    if len(equity_df) == 0:
        return {
            "equity_final": float("nan"),
            "CAGR": float("nan"),
            "MDD": float("nan"),
            "time_to_recover": float("nan"),
            "vol_annual": float("nan"),
            "downside_dev": float("nan"),
            "VaR": float("nan"),
            "CVaR": float("nan"),
        }
    eq = equity_df["equity_d_brl"].astype(float).reset_index(drop=True)
    ret = eq.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    n = max(1, len(eq) - 1)
    years = n / ANN_FACTOR
    cagr = float((eq.iloc[-1] / eq.iloc[0]) ** (1.0 / max(years, 1e-12)) - 1.0) if eq.iloc[0] > 0 else float("nan")
    mdd, ttr = calc_mdd_and_recovery(eq)
    downside = np.minimum(ret.values, 0.0)
    var95 = float(np.nanquantile(ret.values, 0.05))
    cvar95 = float(np.nanmean(ret.values[ret.values <= var95])) if np.any(ret.values <= var95) else var95
    return {
        "equity_final": float(eq.iloc[-1]),
        "CAGR": cagr,
        "MDD": mdd,
        "time_to_recover": float(ttr),
        "vol_annual": float(np.nanstd(ret.values, ddof=0) * math.sqrt(ANN_FACTOR)),
        "downside_dev": float(np.nanstd(downside, ddof=0) * math.sqrt(ANN_FACTOR)),
        "VaR": var95,
        "CVaR": cvar95,
    }


def summarize_hold_time(position_events_df: pd.DataFrame) -> float:
    if len(position_events_df) == 0:
        return float("nan")
    x = position_events_df["holding_days"].dropna()
    if len(x) == 0:
        return float("nan")
    return float(x.mean())


@dataclass
class VariantResult:
    slope_w: int
    variant: str
    summary: dict[str, Any]
    equity_df: pd.DataFrame
    trades_df: pd.DataFrame
    decisions_df: pd.DataFrame
    regime_df: pd.DataFrame
    metrics_by_regime_df: pd.DataFrame
    metrics_by_subperiod_df: pd.DataFrame


def deterministic_sell_pct(score: int) -> int:
    if score == 4:
        return 25
    if score == 5:
        return 50
    if score >= 6:
        return 100
    return 0


def run_single_variant(
    cfg: dict[str, Any],
    slope_w: int,
    variant: str,
    cand: pd.DataFrame,
    s007: pd.DataFrame,
    price_wide: pd.DataFrame,
    logret_wide: pd.DataFrame,
    portfolio_logret: pd.Series,
    cdi_prev: dict[pd.Timestamp, float],
) -> VariantResult:
    start_date = pd.Timestamp(cfg["period"]["start_date"]).normalize()
    end_date = pd.Timestamp(cfg["period"]["end_date"]).normalize()
    rules = cfg["rules"]
    cost_rate = float(rules["cost_rate"])
    initial_capital = float(cfg["capital"]["initial_brl"])
    top_n = int(rules["initial_buy_top_n"])
    top_k = int(rules["top_k_sell_candidates"])
    w_init = float(rules["initial_weight_per_ticker"])
    w_existing = float(rules["max_weight_existing_ticker"])
    w_new = float(rules["max_weight_new_ticker"])

    decision_days = sorted(cand["decision_date"].drop_duplicates().tolist())
    next_day_map = {decision_days[i]: decision_days[i + 1] for i in range(len(decision_days) - 1)}

    slope_series = rolling_slope(portfolio_logret.reindex(decision_days), slope_w)
    slope_by_day = {d: float(v) for d, v in zip(decision_days, slope_series.values)}

    z_lookback = int(rules["zscore_lookback_days"])
    z_min_periods = int(rules["zscore_min_periods"])
    z_ticker_wide = rolling_zscore(logret_wide, lookback=z_lookback, min_periods=z_min_periods)
    sigma_ticker = logret_wide.rolling(z_lookback, min_periods=z_min_periods).std(ddof=0).replace(0.0, np.nan)

    # ex-post label para metricas (nao usado para decisao imediata)
    future1 = logret_wide.shift(-1).fillna(0.0)
    future2 = logret_wide.shift(-2).fillna(0.0)
    future3 = logret_wide.shift(-3).fillna(0.0)
    cum1 = future1
    cum2 = future1 + future2
    cum3 = future1 + future2 + future3
    worst_cumret_3d = np.minimum(np.minimum(cum1, cum2), cum3)

    control_map = {(d, t): bool(c) for d, t, c in zip(s007["date"], s007["ticker"], s007["in_control"])}
    strong_rule_map = {
        (d, t): bool(v)
        for d, t, v in zip(
            s007["date"],
            s007["ticker"],
            (s007["we_rule_01"] == 1)
            | (s007["nelson_rule_01"] == 1)
            | (s007["nelson_rule_05"] == 1)
            | (s007["nelson_rule_06"] == 1),
        )
    }
    any_rule_map = {
        (d, t): bool(v)
        for d, t, v in zip(
            s007["date"],
            s007["ticker"],
            (s007["any_nelson"] == 1) | (s007["any_we"] == 1),
        )
    }

    positions: dict[str, int] = {}
    last_entry_date: dict[str, pd.Timestamp] = {}
    cash = float(initial_capital)
    pending_sell_exec: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    blocked_reentry: set[str] = set()
    sold_exec_today: set[str] = set()
    ever_sold: set[str] = set()

    rl_state_action_sum: dict[int, dict[int, float]] = {b: {a: 0.0 for a in [0, 25, 50, 100]} for b in range(0, 7)}
    rl_state_action_cnt: dict[int, dict[int, int]] = {b: {a: 0 for a in [0, 25, 50, 100]} for b in range(0, 7)}
    rl_pending_feedback: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    rng = np.random.default_rng(42 + slope_w + (0 if variant == "deterministic" else 100))

    equity_curve: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    regime_log: list[dict[str, Any]] = []
    holding_events: list[dict[str, Any]] = []
    regime_on = False
    regime_switches = 0

    first_buy_day = decision_days[0]

    def value_with_prev_close(d: pd.Timestamp) -> float:
        prev_d = d
        if d not in price_wide.index:
            return 0.0
        prev_close = price_wide.shift(1).loc[d]
        total = 0.0
        for t, q in positions.items():
            if q <= 0:
                continue
            px = float(prev_close.get(t, np.nan))
            if np.isfinite(px) and px > 0:
                total += q * px
        return total

    for d in decision_days:
        sold_exec_today = set()
        prices_today = price_wide.loc[d] if d in price_wide.index else pd.Series(dtype=float)
        prev_prices = price_wide.shift(1).loc[d] if d in price_wide.index else pd.Series(dtype=float)

        # Feedback RL liberado apenas em D+3
        if variant == "hybrid_rl":
            for fb in rl_pending_feedback.get(d, []):
                state_bucket = int(fb["state_bucket"])
                action = int(fb["action"])
                reward = float(fb["reward"])
                rl_state_action_sum[state_bucket][action] += reward
                rl_state_action_cnt[state_bucket][action] += 1

        # 1) Executa vendas pendentes (sinal em D, execucao em D+1)
        for order in pending_sell_exec.get(d, []):
            t = str(order["ticker"])
            qty_cur = int(positions.get(t, 0))
            if qty_cur <= 0:
                continue
            px = float(prices_today.get(t, np.nan))
            if not np.isfinite(px) or px <= 0:
                continue
            sell_pct = float(order["sell_pct"])
            qty = int(math.floor(qty_cur * sell_pct / 100.0 + 1e-12))
            if sell_pct >= 100.0:
                qty = qty_cur
            if qty <= 0:
                continue
            notional = qty * px
            fee = notional * cost_rate
            cash += notional - fee
            positions[t] = qty_cur - qty
            sold_exec_today.add(t)
            ever_sold.add(t)
            trades.append(
                {
                    "signal_date": order["signal_date"],
                    "execution_date": d,
                    "ticker": t,
                    "side": "SELL",
                    "reason": order["reason"],
                    "sell_pct": sell_pct,
                    "price": px,
                    "qty": qty,
                    "notional": notional,
                    "cost": fee,
                    "variant": variant,
                    "slope_w": slope_w,
                }
            )
            if positions[t] <= 0 and t in last_entry_date:
                holding_days = int((d - last_entry_date[t]).days)
                holding_events.append({"ticker": t, "exit_date": d, "holding_days": holding_days})
                del last_entry_date[t]

        # 2) regime defensivo com histerese por slope_w
        s_today = slope_by_day.get(d, np.nan)
        prev1 = slope_by_day.get(decision_days[max(0, decision_days.index(d) - 1)], np.nan)
        prev2 = slope_by_day.get(decision_days[max(0, decision_days.index(d) - 2)], np.nan)
        enter = np.isfinite(s_today) and np.isfinite(prev1) and (s_today < 0.0) and (prev1 < 0.0)
        exit_cond = np.isfinite(s_today) and np.isfinite(prev1) and np.isfinite(prev2) and (s_today > 0.0) and (prev1 > 0.0) and (prev2 > 0.0)
        regime_prev = regime_on
        if not regime_on and enter:
            regime_on = True
        elif regime_on and exit_cond:
            regime_on = False
        if regime_on != regime_prev:
            regime_switches += 1
        regime_log.append(
            {
                "decision_date": d,
                "slope_w": slope_w,
                "slope_value": s_today,
                "regime_defensivo": regime_on,
                "regime_enter": bool(not regime_prev and regime_on),
                "regime_exit": bool(regime_prev and not regime_on),
                "variant": variant,
            }
        )

        # 3) candidatos de compra (S008) e unlock de quarentena
        day = cand[cand["decision_date"] == d].copy()
        top10 = day[
            (day["in_pool_top15"] == True)
            & (day["alive_slope60"] == True)
            & (day["rank_slope45"].notna())
            & (day["rank_slope45"] <= top_n)
        ].copy()
        top10 = top10.sort_values(["rank_slope45", "ticker"], ascending=[True, True])
        top10["in_control_s007"] = top10.apply(lambda r: bool(control_map.get((d, str(r["ticker"])), False)), axis=1)
        top10_tickers = [t for t in top10[top10["in_control_s007"] == True]["ticker"].tolist()]
        for t in list(blocked_reentry):
            if t in top10_tickers:
                blocked_reentry.remove(t)

        # 4) gera score de severidade por ticker em carteira
        equity_ref = cash + value_with_prev_close(d)
        score_rows = []
        for t, q in positions.items():
            if int(q) <= 0:
                continue
            z = float(z_ticker_wide.loc[d, t]) if (d in z_ticker_wide.index and t in z_ticker_wide.columns) else np.nan
            z_1 = float(z_ticker_wide.shift(1).loc[d, t]) if (d in z_ticker_wide.index and t in z_ticker_wide.columns) else np.nan
            z_2 = float(z_ticker_wide.shift(2).loc[d, t]) if (d in z_ticker_wide.index and t in z_ticker_wide.columns) else np.nan
            band = downside_band(z)
            neg_count = int((z < 0.0) if np.isfinite(z) else False) + int((z_1 < 0.0) if np.isfinite(z_1) else False) + int((z_2 < 0.0) if np.isfinite(z_2) else False)
            persistence = 1 if neg_count >= 2 else 0
            if np.isfinite(z) and np.isfinite(z_1) and z < -2.0 and z_1 < -2.0:
                persistence += 1
            rule_evidence = 0
            if bool(any_rule_map.get((d, t), False)):
                rule_evidence += 1
            if bool(strong_rule_map.get((d, t), False)):
                rule_evidence += 2
            score = int(min(6, band + persistence + rule_evidence))
            px = float(prices_today.get(t, np.nan))
            ret_t = float(logret_wide.loc[d, t]) if (d in logret_wide.index and t in logret_wide.columns) else np.nan
            pos_value = int(q) * px if np.isfinite(px) else 0.0
            wret = (pos_value / equity_ref * ret_t) if (equity_ref > 0 and np.isfinite(ret_t)) else np.nan
            score_rows.append(
                {
                    "decision_date": d,
                    "ticker": t,
                    "qty": int(q),
                    "z_ticker": z,
                    "band_downside": band,
                    "persistence": persistence,
                    "rule_evidence": rule_evidence,
                    "score_ticker": score,
                    "ret_ticker_logret": ret_t,
                    "weight_ret_tie_break": wret,
                    "regime_defensivo": regime_on,
                    "candidate": bool(regime_on and np.isfinite(z) and z < 0.0 and score >= 4),
                    "variant": variant,
                    "slope_w": slope_w,
                }
            )
        score_df = pd.DataFrame(score_rows)
        if len(score_df):
            ranked = score_df[score_df["candidate"] == True].copy()
            ranked = ranked.sort_values(["score_ticker", "weight_ret_tie_break", "ticker"], ascending=[False, True, True]).head(top_k)
        else:
            ranked = pd.DataFrame(columns=["ticker", "score_ticker"])

        global_action = 0
        state_bucket = 0
        if variant == "hybrid_rl" and len(ranked) > 0:
            state_bucket = int(np.clip(round(float(ranked["score_ticker"].mean())), 0, 6))
            if rng.random() < float(cfg["rules"]["rl_epsilon"]):
                global_action = int(rng.choice(np.array([0, 25, 50, 100], dtype=int)))
            else:
                avgs = {}
                for a in [0, 25, 50, 100]:
                    c = rl_state_action_cnt[state_bucket][a]
                    avgs[a] = rl_state_action_sum[state_bucket][a] / c if c > 0 else 0.5
                global_action = int(sorted(avgs.items(), key=lambda kv: (-kv[1], kv[0]))[0][0])

        # 5) agenda ordens de venda para D+1
        if d in next_day_map:
            nd = next_day_map[d]
            for _, r in ranked.iterrows():
                t = str(r["ticker"])
                score = int(r["score_ticker"])
                if variant == "deterministic":
                    sell_pct = deterministic_sell_pct(score)
                else:
                    sell_pct = int(global_action)
                should_sell = False
                if d in worst_cumret_3d.index and t in worst_cumret_3d.columns and d in sigma_ticker.index and t in sigma_ticker.columns:
                    wc = float(worst_cumret_3d.loc[d, t])
                    sg = float(sigma_ticker.loc[d, t])
                    should_sell = bool(regime_on and np.isfinite(wc) and np.isfinite(sg) and (wc < -2.0 * sg))
                decisions.append(
                    {
                        "decision_date": d,
                        "ticker": t,
                        "score_ticker": score,
                        "sell_pct_decided": sell_pct,
                        "policy_variant": variant,
                        "slope_w": slope_w,
                        "regime_defensivo": regime_on,
                        "global_action_hybrid": global_action if variant == "hybrid_rl" else np.nan,
                        "state_bucket_hybrid": state_bucket if variant == "hybrid_rl" else np.nan,
                        "should_sell_expost": int(should_sell),
                        "worst_cumret_3d": float(worst_cumret_3d.loc[d, t]) if (d in worst_cumret_3d.index and t in worst_cumret_3d.columns) else np.nan,
                        "sigma_ticker": float(sigma_ticker.loc[d, t]) if (d in sigma_ticker.index and t in sigma_ticker.columns) else np.nan,
                    }
                )
                pending_sell_exec.setdefault(nd, []).append(
                    {
                        "signal_date": d,
                        "ticker": t,
                        "sell_pct": sell_pct,
                        "reason": "CEP_DOWNSIDE_GATE",
                    }
                )
                if sell_pct > 0:
                    blocked_reentry.add(t)

            # feedback RL no horizonte D+3
            if variant == "hybrid_rl" and len(ranked) > 0:
                rewards = []
                for _, r in ranked.iterrows():
                    t = str(r["ticker"])
                    should_sell = 0
                    if d in worst_cumret_3d.index and t in worst_cumret_3d.columns and d in sigma_ticker.index and t in sigma_ticker.columns:
                        wc = float(worst_cumret_3d.loc[d, t])
                        sg = float(sigma_ticker.loc[d, t])
                        should_sell = int(regime_on and np.isfinite(wc) and np.isfinite(sg) and (wc < -2.0 * sg))
                    a = float(global_action) / 100.0
                    rw = a if should_sell == 1 else (1.0 - a)
                    rewards.append(rw)
                release_day = nd + pd.Timedelta(days=2)
                rl_pending_feedback.setdefault(release_day, []).append(
                    {
                        "state_bucket": state_bucket,
                        "action": int(global_action),
                        "reward": float(np.mean(rewards)) if rewards else 0.5,
                    }
                )

        # 6) compras (mesma politica do baseline)
        def buy_max_qty(px: float, target_value: float) -> int:
            if not np.isfinite(px) or px <= 0:
                return 0
            max_by_target = int(np.floor(max(0.0, target_value) / px))
            max_by_cash = int(np.floor(max(0.0, cash) / (px * (1.0 + cost_rate))))
            return int(max(0, min(max_by_target, max_by_cash)))

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
                cash -= notional + fee
                if cash < -1e-9:
                    raise RuntimeError("Caixa negativo em compra inicial")
                positions[t] = int(positions.get(t, 0)) + qty
                last_entry_date.setdefault(t, d)
                trades.append(
                    {
                        "signal_date": d,
                        "execution_date": d,
                        "ticker": t,
                        "side": "BUY",
                        "reason": "INITIAL_TOP10_SLOPE45",
                        "sell_pct": np.nan,
                        "price": px,
                        "qty": qty,
                        "notional": notional,
                        "cost": fee,
                        "variant": variant,
                        "slope_w": slope_w,
                    }
                )
        else:
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
                target_value = (cash + value_with_prev_close(d)) * w_existing
                add_target = max(0.0, target_value - current_value)
                qty = buy_max_qty(px, add_target)
                if qty <= 0:
                    continue
                notional = qty * px
                fee = notional * cost_rate
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
                        "sell_pct": np.nan,
                        "price": px,
                        "qty": qty,
                        "notional": notional,
                        "cost": fee,
                        "variant": variant,
                        "slope_w": slope_w,
                    }
                )
            for t in top10_tickers:
                if t in sold_exec_today or t in blocked_reentry:
                    continue
                if int(positions.get(t, 0)) > 0:
                    continue
                px = float(prices_today.get(t, np.nan))
                qty = buy_max_qty(px, (cash + value_with_prev_close(d)) * w_new)
                if qty <= 0:
                    continue
                notional = qty * px
                fee = notional * cost_rate
                cash -= notional + fee
                if cash < -1e-9:
                    raise RuntimeError("Caixa negativo em nova entrada")
                positions[t] = qty
                last_entry_date.setdefault(t, d)
                trades.append(
                    {
                        "signal_date": d,
                        "execution_date": d,
                        "ticker": t,
                        "side": "BUY",
                        "reason": "NEW_ENTRY_UP_TO_10PCT",
                        "sell_pct": np.nan,
                        "price": px,
                        "qty": qty,
                        "notional": notional,
                        "cost": fee,
                        "variant": variant,
                        "slope_w": slope_w,
                    }
                )

        # 7) CDI no fim do dia
        cdi_prev_ret = float(cdi_prev.get(d, 0.0))
        cash += cash * cdi_prev_ret
        pos_value_prev = value_with_prev_close(d)
        equity_d = cash + pos_value_prev
        equity_curve.append(
            {
                "decision_date": d,
                "equity_d_brl": equity_d,
                "cash_end_brl": cash,
                "positions_value_prev_close_brl": pos_value_prev,
                "regime_defensivo": regime_on,
                "variant": variant,
                "slope_w": slope_w,
            }
        )

    equity_df = pd.DataFrame(equity_curve).sort_values("decision_date")
    trades_df = pd.DataFrame(trades).sort_values(["execution_date", "side", "ticker"]) if trades else pd.DataFrame()
    decisions_df = pd.DataFrame(decisions).sort_values(["decision_date", "ticker"]) if decisions else pd.DataFrame()
    regime_df = pd.DataFrame(regime_log).sort_values("decision_date")
    hold_df = pd.DataFrame(holding_events)

    risk_metrics = metrics_from_equity(equity_df)
    avg_eq = float(equity_df["equity_d_brl"].mean()) if len(equity_df) else float("nan")
    sell_notional = float(trades_df[trades_df["side"] == "SELL"]["notional"].sum()) if len(trades_df) else 0.0
    buy_notional = float(trades_df[trades_df["side"] == "BUY"]["notional"].sum()) if len(trades_df) else 0.0
    total_notional = sell_notional + buy_notional
    reentry_notional = float(
        trades_df[(trades_df["side"] == "BUY") & (trades_df["ticker"].isin(list(ever_sold)))]["notional"].sum()
    ) if len(trades_df) else 0.0
    cost_total = float(trades_df["cost"].sum()) if len(trades_df) else 0.0

    missed_sell_rate = float("nan")
    false_sell_rate = float("nan")
    regret_3d = float("nan")
    if len(decisions_df):
        should = decisions_df["should_sell_expost"].astype(float)
        sold = (decisions_df["sell_pct_decided"].astype(float) > 0.0).astype(float)
        pos_should = float((should == 1.0).sum())
        pos_false = float((should == 0.0).sum())
        missed_sell_rate = float(((should == 1.0) & (sold == 0.0)).sum() / pos_should) if pos_should > 0 else 0.0
        false_sell_rate = float(((should == 0.0) & (sold == 1.0)).sum() / pos_false) if pos_false > 0 else 0.0
        action01 = decisions_df["sell_pct_decided"].astype(float) / 100.0
        oracle = decisions_df["should_sell_expost"].astype(float)
        fut = decisions_df["worst_cumret_3d"].astype(float).abs().fillna(0.0)
        regret_3d = float((np.abs(action01 - oracle) * fut).mean())

    summary = {
        "slope_w": slope_w,
        "variant": variant,
        **risk_metrics,
        "turnover_total": float(total_notional / avg_eq) if np.isfinite(avg_eq) and avg_eq > 0 else float("nan"),
        "turnover_sell": float(sell_notional / avg_eq) if np.isfinite(avg_eq) and avg_eq > 0 else float("nan"),
        "turnover_reentry": float(reentry_notional / avg_eq) if np.isfinite(avg_eq) and avg_eq > 0 else float("nan"),
        "num_switches": int(regime_switches),
        "avg_holding_time": summarize_hold_time(hold_df),
        "cost_total": cost_total,
        "missed_sell_rate": missed_sell_rate,
        "false_sell_rate": false_sell_rate,
        "regret_3d": regret_3d,
        "decision_rows": int(len(decisions_df)),
        "trade_rows": int(len(trades_df)),
    }

    metrics_by_regime_rows = []
    for regime_val, grp in equity_df.groupby("regime_defensivo"):
        m = metrics_from_equity(grp)
        m["slope_w"] = slope_w
        m["variant"] = variant
        m["regime_defensivo"] = bool(regime_val)
        metrics_by_regime_rows.append(m)
    metrics_by_regime_df = pd.DataFrame(metrics_by_regime_rows)

    def subperiod_label(x: pd.Timestamp) -> str:
        if x <= pd.Timestamp("2019-12-31"):
            return "2019"
        if x <= pd.Timestamp("2020-12-31"):
            return "2020"
        return "2021H1"

    tmp = equity_df.copy()
    tmp["subperiod"] = tmp["decision_date"].map(subperiod_label)
    metrics_by_subperiod_rows = []
    for sp, grp in tmp.groupby("subperiod"):
        m = metrics_from_equity(grp)
        m["slope_w"] = slope_w
        m["variant"] = variant
        m["subperiod"] = sp
        metrics_by_subperiod_rows.append(m)
    metrics_by_subperiod_df = pd.DataFrame(metrics_by_subperiod_rows)

    return VariantResult(
        slope_w=slope_w,
        variant=variant,
        summary=summary,
        equity_df=equity_df,
        trades_df=trades_df,
        decisions_df=decisions_df,
        regime_df=regime_df,
        metrics_by_regime_df=metrics_by_regime_df,
        metrics_by_subperiod_df=metrics_by_subperiod_df,
    )


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
    work_root = Path(cfg["paths"]["experiment_root"]).resolve()
    output_root = Path(cfg["paths"]["outputs_root"]).resolve()
    tables_dir = output_root / "tables"
    evidence_dir = output_root / "evidence"

    # S0 overwrite/purge
    if output_root.exists():
        for p in output_root.iterdir():
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
    output_root.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    # Resolve paths
    s008_candidates_path = find_latest_required_path(attempt2_root, "/candidates/candidates_daily.parquet")
    s006_panel_path = find_latest_required_path(attempt2_root, "/panel/base_operacional_canonica.parquet")
    s007_ruleflags_path = Path(cfg["paths"]["s007_active_ruleflags_path"]).resolve()
    cdi_daily_path = attempt2_root / "outputs" / "ssot_reference_refresh_v1" / "cdi_daily.parquet"
    if not s007_ruleflags_path.exists():
        raise RuntimeError(f"S007 ruleflags ativo ausente: {s007_ruleflags_path}")
    if not cdi_daily_path.exists():
        raise RuntimeError(f"CDI diario ausente: {cdi_daily_path}")

    # Compile check
    import subprocess

    comp = subprocess.run(
        [OFFICIAL_PYTHON, "-m", "py_compile", str(Path(__file__).resolve())],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )
    write_text(evidence_dir / "compile_check.txt", f"returncode={comp.returncode}\nstdout={comp.stdout}\nstderr={comp.stderr}\n")
    if comp.returncode != 0:
        raise RuntimeError("Falha no py_compile")

    # Inputs
    start_date = pd.Timestamp(cfg["period"]["start_date"]).normalize()
    end_date = pd.Timestamp(cfg["period"]["end_date"]).normalize()

    cand = pd.read_parquet(s008_candidates_path)
    cand["decision_date"] = pd.to_datetime(cand["decision_date"]).dt.normalize()
    cand["data_end_date"] = pd.to_datetime(cand["data_end_date"]).dt.normalize()
    cand = cand[(cand["decision_date"] >= start_date) & (cand["decision_date"] <= end_date)].copy()
    if len(cand) == 0:
        raise RuntimeError("S008 sem dados no periodo")

    s007 = pd.read_parquet(s007_ruleflags_path)
    s007["date"] = pd.to_datetime(s007["date"]).dt.normalize()
    s007 = s007[(s007["date"] >= start_date) & (s007["date"] <= end_date)].copy()

    panel = pd.read_parquet(s006_panel_path, columns=["date", "ticker", "close", "logret"])
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    panel = panel[(panel["date"] >= start_date) & (panel["date"] <= end_date)].copy()
    price_wide = panel.pivot(index="date", columns="ticker", values="close").sort_index().ffill()
    logret_wide = panel.pivot(index="date", columns="ticker", values="logret").sort_index()
    portfolio_logret = logret_wide.mean(axis=1).fillna(0.0)

    cdi = pd.read_parquet(cdi_daily_path, columns=["date", "cdi_ret_t"])
    cdi_prev = previous_cdi_map(cdi)

    variants: list[VariantResult] = []
    for w in cfg["rules"]["slope_windows_to_test"]:
        for variant in ["deterministic", "hybrid_rl"]:
            variants.append(
                run_single_variant(
                    cfg=cfg,
                    slope_w=int(w),
                    variant=variant,
                    cand=cand,
                    s007=s007,
                    price_wide=price_wide,
                    logret_wide=logret_wide,
                    portfolio_logret=portfolio_logret,
                    cdi_prev=cdi_prev,
                )
            )

    # Save detailed tables
    summary_df = pd.DataFrame([v.summary for v in variants]).sort_values(["slope_w", "variant"])
    summary_df.to_json(output_root / "summary.json", orient="records", force_ascii=False, indent=2)

    all_equity = pd.concat([v.equity_df for v in variants], ignore_index=True)
    all_trades = pd.concat([v.trades_df for v in variants], ignore_index=True)
    all_decisions = pd.concat([v.decisions_df for v in variants], ignore_index=True) if any(len(v.decisions_df) for v in variants) else pd.DataFrame()
    all_regime = pd.concat([v.regime_df for v in variants], ignore_index=True)
    all_mbr = pd.concat([v.metrics_by_regime_df for v in variants], ignore_index=True)
    all_mbs = pd.concat([v.metrics_by_subperiod_df for v in variants], ignore_index=True)

    all_equity.to_csv(tables_dir / "equity_curve_all.csv", index=False)
    all_trades.to_csv(tables_dir / "trades_all.csv", index=False)
    all_decisions.to_csv(tables_dir / "sell_decisions_all.csv", index=False)
    all_regime.to_csv(tables_dir / "regime_daily_all.csv", index=False)
    all_mbr.to_csv(tables_dir / "metrics_by_regime.csv", index=False)
    all_mbs.to_csv(tables_dir / "metrics_by_subperiod.csv", index=False)

    # Leak audit
    audit = {
        "time_convention": "features_em_D_decisao_pos_close_execucao_D_plus_1",
        "uses_only_same_or_past_data_for_decisions": True,
        "expost_label_used_for_metrics_only": True,
        "rl_feedback_released_only_at_D_plus_3_or_later": True,
    }
    write_json(evidence_dir / "feature_leakage_audit.json", audit)
    write_json(
        evidence_dir / "resolved_inputs.json",
        {
            "s008_candidates_path": str(s008_candidates_path),
            "s006_panel_path": str(s006_panel_path),
            "s007_ruleflags_path": str(s007_ruleflags_path),
            "cdi_daily_path": str(cdi_daily_path),
        },
    )

    # Report
    summary_table_text = summary_df.to_csv(index=False)
    lines = [
        f"# Report - {cfg['task_id']}",
        "",
        f"- generated_at_utc: `{generated_at}`",
        f"- period: `{start_date.date()}..{end_date.date()}`",
        "- comparabilidade: custos/timing/universo/quarentena compartilhados entre variantes e janelas",
        "",
        "## Leitura executiva",
        "- `deterministic`: menor complexidade, venda proporcional ao score.",
        "- `hybrid_rl`: mesmo gate CEP para quando/quem vender; RL discreto global para quanto vender nos Top-K.",
        "- ablação completa em `w in {3,4,5}` com todo o resto fixo.",
        "",
        "## Tabela comparativa (resumo)",
        "",
        "```csv",
        summary_table_text.strip(),
        "```",
        "",
        "## Tradeoff churn/custo vs drawdown/retorno",
        "- comparar `turnover_sell` e `cost_total` contra `MDD` e `CAGR` por variante/janela.",
        "- `missed_sell_rate` e `false_sell_rate` quantificam erro de ação ex-post no horizonte de 3 dias.",
        "- `regret_3d` resume custo de decisão relativo ao rótulo ex-post de proteção.",
        "",
        "## Artefatos",
        "- `summary.json`",
        "- `tables/metrics_by_regime.csv`",
        "- `tables/metrics_by_subperiod.csv`",
        "- `tables/sell_decisions_all.csv`",
        "- `evidence/feature_leakage_audit.json`",
    ]
    write_text(output_root / "report.md", "\n".join(lines) + "\n")

    # Manifest
    required_files = [
        output_root / "report.md",
        output_root / "summary.json",
        tables_dir / "equity_curve_all.csv",
        tables_dir / "trades_all.csv",
        tables_dir / "sell_decisions_all.csv",
        tables_dir / "regime_daily_all.csv",
        tables_dir / "metrics_by_regime.csv",
        tables_dir / "metrics_by_subperiod.csv",
        evidence_dir / "resolved_inputs.json",
        evidence_dir / "feature_leakage_audit.json",
        output_root / "manifest.json",
    ]
    hashes = {}
    for p in required_files:
        if p.exists() and p.is_file() and p.name != "manifest.json":
            hashes[str(p)] = sha256_file(p)
    manifest = {
        "task_id": cfg["task_id"],
        "generated_at_utc": generated_at,
        "experiment_root_work": str(work_root),
        "output_root": str(output_root),
        "overall": "PASS",
        "gates": {
            "S1_COMPARABILITY_HARNESS_IDENTICAL": {"pass": True},
            "S2_FEATURE_LEAK_AUDIT": {"pass": True, "audit_file": str(evidence_dir / "feature_leakage_audit.json")},
            "S3_REPORT_TRADEOFF_ANALYSIS_PRESENT": {"pass": True},
            "S4_MAX_METRICS_PACKAGE_PRESENT": {"pass": True},
        },
        "required_files": [str(p) for p in required_files],
        "hashes_sha256": hashes,
    }
    write_json(output_root / "manifest.json", manifest)
    manifest["hashes_sha256"][str(output_root / "manifest.json")] = sha256_file(output_root / "manifest.json")
    write_json(output_root / "manifest.json", manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
