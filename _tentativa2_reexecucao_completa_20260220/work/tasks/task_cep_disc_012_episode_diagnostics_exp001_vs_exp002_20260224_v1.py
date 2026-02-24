from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


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


def parse_variant(spec: str) -> tuple[str, int]:
    # expected deterministic_w4 / hybrid_rl_w5
    if spec.endswith("_w3") or spec.endswith("_w4") or spec.endswith("_w5"):
        w = int(spec[-1])
        variant = spec[: -3]
        return variant, w
    raise ValueError(f"Formato de variante invalido: {spec}")


def load_task_spec(path: Path) -> dict[str, Any]:
    # YAML no projeto e armazenado como JSON valido.
    return read_json(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-spec", required=True)
    args = parser.parse_args()

    task_spec = load_task_spec(Path(args.task_spec).resolve())
    repo_root = Path(task_spec["repo_root"]).resolve()
    working_root = Path(task_spec["working_root"]).resolve()
    outputs_dir = (working_root / task_spec["agnostic_requirements"]["outputs_dir"]).resolve()
    shared_tables_dir = outputs_dir / "shared" / "tables"
    run_root = (repo_root / task_spec["agnostic_requirements"]["run_artifacts_root"]).resolve()
    run_dir = run_root / f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    shared_tables_dir.mkdir(parents=True, exist_ok=True)

    exp001_root = (working_root / task_spec["inputs"]["exp001_root"]).resolve()
    exp002_root = (working_root / task_spec["inputs"]["exp002_root"]).resolve()
    variant_name, variant_w = parse_variant(task_spec["inputs"]["exp002_variant_to_explain"])

    gates: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []

    # S1 allowlist
    s1_ok = (
        str(outputs_dir).startswith(str(working_root / "outputs"))
        and str(exp001_root).startswith(str(working_root / "outputs"))
        and str(exp002_root).startswith(str(working_root / "outputs"))
        and str(run_dir).startswith(str(repo_root / "planning/runs"))
    )
    gates.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if s1_ok else "FAIL"})
    steps.append({"name": "S1_GATE_ALLOWLIST_PATHS", "status": "PASS" if s1_ok else "FAIL"})
    if not s1_ok:
        report = {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps}
        write_json(run_dir / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 1

    # S2 locate inputs + canonical verify
    required = [
        exp001_root / "equity_curve.parquet",
        exp001_root / "trades.parquet",
        exp001_root / "positions_daily.parquet",
        exp001_root / "ledger_daily.parquet",
        exp002_root / "tables/equity_curve_all.parquet",
        exp002_root / "tables/trades_all.parquet",
        exp002_root / "tables/sell_decisions_all.parquet",
        exp002_root / "summary.json",
        exp002_root / "report.md",
    ]
    s2_ok = all(p.exists() for p in required)
    gates.append({"name": "S2_LOCATE_INPUTS_AND_VERIFY_CANONICAL_PATHS", "status": "PASS" if s2_ok else "FAIL"})
    steps.append({"name": "S2_LOCATE_INPUTS_AND_VERIFY_CANONICAL_PATHS", "status": "PASS" if s2_ok else "FAIL"})
    if not s2_ok:
        report = {"task_id": task_spec["instruction_id"], "overall_pass": False, "gates": gates, "steps": steps}
        write_json(run_dir / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 1

    # S3 parquet-first extract normalize
    exp001_equity = pd.read_parquet(exp001_root / "equity_curve.parquet").copy()
    exp001_trades = pd.read_parquet(exp001_root / "trades.parquet").copy()
    exp001_positions = pd.read_parquet(exp001_root / "positions_daily.parquet").copy()
    exp001_ledger = pd.read_parquet(exp001_root / "ledger_daily.parquet").copy()
    exp001_signals = pd.read_parquet(exp001_root / "signals_used.parquet").copy()

    exp002_equity_all = pd.read_parquet(exp002_root / "tables/equity_curve_all.parquet").copy()
    exp002_trades_all = pd.read_parquet(exp002_root / "tables/trades_all.parquet").copy()
    exp002_decisions_all = pd.read_parquet(exp002_root / "tables/sell_decisions_all.parquet").copy()
    exp002_regime_all = pd.read_parquet(exp002_root / "tables/regime_daily_all.parquet").copy()

    exp001_equity["decision_date"] = pd.to_datetime(exp001_equity["decision_date"]).dt.normalize()
    exp001_trades["execution_date"] = pd.to_datetime(exp001_trades["execution_date"]).dt.normalize()
    exp001_trades["signal_date"] = pd.to_datetime(exp001_trades["signal_date"]).dt.normalize()
    exp001_positions["decision_date"] = pd.to_datetime(exp001_positions["decision_date"]).dt.normalize()
    exp001_ledger["decision_date"] = pd.to_datetime(exp001_ledger["decision_date"]).dt.normalize()
    exp001_signals["decision_date"] = pd.to_datetime(exp001_signals["decision_date"]).dt.normalize()

    exp002_equity_all["decision_date"] = pd.to_datetime(exp002_equity_all["decision_date"]).dt.normalize()
    exp002_trades_all["execution_date"] = pd.to_datetime(exp002_trades_all["execution_date"]).dt.normalize()
    exp002_trades_all["signal_date"] = pd.to_datetime(exp002_trades_all["signal_date"]).dt.normalize()
    exp002_decisions_all["decision_date"] = pd.to_datetime(exp002_decisions_all["decision_date"]).dt.normalize()
    exp002_regime_all["decision_date"] = pd.to_datetime(exp002_regime_all["decision_date"]).dt.normalize()

    exp002_equity = exp002_equity_all[(exp002_equity_all["variant"] == variant_name) & (exp002_equity_all["slope_w"] == variant_w)].copy()
    exp002_trades = exp002_trades_all[(exp002_trades_all["variant"] == variant_name) & (exp002_trades_all["slope_w"] == variant_w)].copy()
    exp002_decisions = exp002_decisions_all[
        (exp002_decisions_all["policy_variant"] == variant_name) & (exp002_decisions_all["slope_w"] == variant_w)
    ].copy()
    exp002_regime = exp002_regime_all[(exp002_regime_all["variant"] == variant_name) & (exp002_regime_all["slope_w"] == variant_w)].copy()

    # Ruleflags fonte canônica global
    s007_ruleflags = pd.read_parquet(
        working_root / "outputs/state/s007_ruleflags_global/20260223/s007_ruleflags.parquet"
    )
    s007_ruleflags["date"] = pd.to_datetime(s007_ruleflags["date"]).dt.normalize()

    # Precos para reconstruir positions_daily exp002
    resolved_inputs = read_json(exp002_root / "evidence/resolved_inputs.json")
    s006_panel_path = Path(resolved_inputs["s006_panel_path"])
    panel = pd.read_parquet(s006_panel_path, columns=["date", "ticker", "close"])
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    close_wide = panel.pivot(index="date", columns="ticker", values="close").sort_index().ffill()
    close_prev = close_wide.shift(1)

    # Reconstrucao de posicoes EXP002 (apenas variante escolhida)
    decision_days = sorted(exp002_equity["decision_date"].drop_duplicates().tolist())
    trades_by_day = {d: g for d, g in exp002_trades.groupby("execution_date")}
    pos: dict[str, int] = {}
    rec_rows = []
    for d in decision_days:
        day_trades = trades_by_day.get(d, pd.DataFrame())
        if len(day_trades):
            for _, tr in day_trades.sort_values(["side", "ticker"]).iterrows():
                t = str(tr["ticker"])
                q = int(tr["qty"])
                if str(tr["side"]).upper() == "BUY":
                    pos[t] = int(pos.get(t, 0)) + q
                else:
                    pos[t] = int(pos.get(t, 0)) - q
                    if pos[t] <= 0:
                        pos[t] = 0
        if d in close_prev.index:
            cp = close_prev.loc[d]
            for t, q in pos.items():
                if q <= 0:
                    continue
                px = float(cp.get(t, np.nan))
                if not np.isfinite(px):
                    continue
                rec_rows.append(
                    {
                        "decision_date": d,
                        "ticker": t,
                        "qty": int(q),
                        "close_d_1": px,
                        "value_prev_close_brl": float(q * px),
                    }
                )
    exp002_positions = pd.DataFrame(rec_rows)

    # persist shared tables
    tables = {
        "exp001_equity.parquet": exp001_equity,
        "exp001_trades.parquet": exp001_trades,
        "exp001_positions_daily.parquet": exp001_positions,
        "exp001_ledger_daily.parquet": exp001_ledger,
        "exp001_signals_used.parquet": exp001_signals,
        "exp002_equity_variant.parquet": exp002_equity,
        "exp002_trades_variant.parquet": exp002_trades,
        "exp002_decisions_variant.parquet": exp002_decisions,
        "exp002_regime_variant.parquet": exp002_regime,
        "exp002_positions_reconstructed.parquet": exp002_positions,
        "s007_ruleflags_global.parquet": s007_ruleflags,
    }
    for fn, df in tables.items():
        df.to_parquet(shared_tables_dir / fn, index=False)
    s3_ok = all((shared_tables_dir / k).exists() for k in tables.keys())
    gates.append({"name": "S3_EXTRACT_AND_NORMALIZE_TABLES_PARQUET_FIRST", "status": "PASS" if s3_ok else "FAIL", "details": {"tables": len(tables)}})
    steps.append({"name": "S3_EXTRACT_AND_NORMALIZE_TABLES_PARQUET_FIRST", "status": "PASS" if s3_ok else "FAIL"})

    # S4 reconcile equity + costs
    def reconcile_df(eq_df: pd.DataFrame, tr_df: pd.DataFrame, label: str) -> pd.DataFrame:
        x = eq_df[["decision_date", "equity_d_brl", "cash_end_brl", "positions_value_prev_close_brl"]].copy().sort_values("decision_date")
        x["delta_equity"] = x["equity_d_brl"].diff().fillna(0.0)
        x["delta_cash"] = x["cash_end_brl"].diff().fillna(0.0)
        x["delta_positions_value"] = x["positions_value_prev_close_brl"].diff().fillna(0.0)
        x["residual_delta"] = x["delta_equity"] - (x["delta_cash"] + x["delta_positions_value"])
        x["identity_residual"] = x["equity_d_brl"] - (x["cash_end_brl"] + x["positions_value_prev_close_brl"])
        c = tr_df.groupby("execution_date", as_index=False)["cost"].sum().rename(columns={"execution_date": "decision_date", "cost": "trade_cost_brl"})
        x = x.merge(c, on="decision_date", how="left")
        x["trade_cost_brl"] = x["trade_cost_brl"].fillna(0.0)
        x["exp_id"] = label
        return x

    rec1 = reconcile_df(exp001_equity, exp001_trades, "EXP_001")
    rec2 = reconcile_df(exp002_equity, exp002_trades, "EXP_002")
    rec = pd.concat([rec1, rec2], ignore_index=True)
    rec.to_parquet(shared_tables_dir / "equity_reconciliation_daily.parquet", index=False)

    # comparativo equity para reproduzir grafico
    cmp = (
        rec1[["decision_date", "equity_d_brl", "positions_value_prev_close_brl"]]
        .rename(columns={"equity_d_brl": "exp001_equity_brl", "positions_value_prev_close_brl": "exp001_valor_ativo_brl"})
        .merge(
            rec2[["decision_date", "equity_d_brl", "positions_value_prev_close_brl"]].rename(
                columns={"equity_d_brl": "exp002_equity_brl", "positions_value_prev_close_brl": "exp002_valor_ativo_brl"}
            ),
            on="decision_date",
            how="inner",
        )
        .sort_values("decision_date")
    )
    cmp["delta_exp002_minus_exp001"] = cmp["exp002_equity_brl"] - cmp["exp001_equity_brl"]
    cmp["delta_daily_change"] = cmp["delta_exp002_minus_exp001"].diff().fillna(0.0)
    cmp.to_parquet(shared_tables_dir / "equity_compare_exp001_vs_exp002.parquet", index=False)

    tol = 1e-6
    s4_ok = (
        float(rec["identity_residual"].abs().max()) <= tol
        and float(rec["residual_delta"].abs().max()) <= tol
        and len(cmp) > 0
    )
    gates.append(
        {
            "name": "S4_RECONCILE_EQUITY_AND_COSTS",
            "status": "PASS" if s4_ok else "FAIL",
            "details": {
                "max_identity_residual_abs": float(rec["identity_residual"].abs().max()),
                "max_delta_residual_abs": float(rec["residual_delta"].abs().max()),
                "tolerance": tol,
            },
        }
    )
    steps.append({"name": "S4_RECONCILE_EQUITY_AND_COSTS", "status": "PASS" if s4_ok else "FAIL"})

    # Attribution helpers
    exp001_stress = exp001_trades[
        (exp001_trades["side"] == "SELL") & (exp001_trades["reason"].astype(str).str.contains("STRESS", na=False))
    ].copy()
    exp001_stress["decision_date"] = exp001_stress["signal_date"]
    exp001_stress["action_exp001"] = "STRESS_SELL_100_D_PLUS_1"
    exp001_stress_rule = exp001_stress.merge(
        s007_ruleflags,
        left_on=["decision_date", "ticker"],
        right_on=["date", "ticker"],
        how="left",
        suffixes=("", "_rf"),
    )
    exp002_dec_rule = exp002_decisions.merge(
        s007_ruleflags,
        left_on=["decision_date", "ticker"],
        right_on=["date", "ticker"],
        how="left",
        suffixes=("", "_rf"),
    )
    exp001_stress_rule.to_parquet(shared_tables_dir / "exp001_stress_rule_attribution.parquet", index=False)
    exp002_dec_rule.to_parquet(shared_tables_dir / "exp002_decisions_rule_attribution.parquet", index=False)

    # ticker contribution by day (value change)
    p1 = exp001_positions.copy()
    p1["value_prev_close_brl"] = p1["qty"].astype(float) * p1["close_d_1"].astype(float)
    p1 = p1[["decision_date", "ticker", "value_prev_close_brl"]]
    p2 = exp002_positions[["decision_date", "ticker", "value_prev_close_brl"]].copy()
    all_days = sorted(cmp["decision_date"].tolist())
    idx = pd.MultiIndex.from_product([all_days, sorted(set(p1["ticker"]).union(set(p2["ticker"])))], names=["decision_date", "ticker"])
    wide = (
        pd.DataFrame(index=idx)
        .reset_index()
        .merge(p1.rename(columns={"value_prev_close_brl": "v1"}), on=["decision_date", "ticker"], how="left")
        .merge(p2.rename(columns={"value_prev_close_brl": "v2"}), on=["decision_date", "ticker"], how="left")
        .fillna(0.0)
        .sort_values(["ticker", "decision_date"])
    )
    wide["dv1"] = wide.groupby("ticker")["v1"].diff().fillna(0.0)
    wide["dv2"] = wide.groupby("ticker")["v2"].diff().fillna(0.0)
    wide["delta_value_diff"] = wide["dv2"] - wide["dv1"]
    wide.to_parquet(shared_tables_dir / "ticker_contribution_daily.parquet", index=False)

    # S5 episode reports
    episodes = task_spec["inputs"]["episodes"]
    generated_reports = []
    for ep in episodes:
        eid = ep["episode_id"]
        d0 = pd.Timestamp(ep["date_start"]).normalize()
        d1 = pd.Timestamp(ep["date_end"]).normalize()
        ep_dir = outputs_dir / eid
        ep_dir.mkdir(parents=True, exist_ok=True)
        ep_cmp = cmp[(cmp["decision_date"] >= d0) & (cmp["decision_date"] <= d1)].copy()
        ep_rec = rec[(rec["decision_date"] >= d0) & (rec["decision_date"] <= d1)].copy()
        ep_wide = wide[(wide["decision_date"] >= d0) & (wide["decision_date"] <= d1)].copy()
        ep_dec = exp002_dec_rule[(exp002_dec_rule["decision_date"] >= d0) & (exp002_dec_rule["decision_date"] <= d1)].copy()
        ep_stress = exp001_stress_rule[(exp001_stress_rule["decision_date"] >= d0) & (exp001_stress_rule["decision_date"] <= d1)].copy()

        top_days = ep_cmp.reindex(ep_cmp["delta_daily_change"].abs().sort_values(ascending=False).index).head(8)
        top_days.to_parquet(ep_dir / "top_delta_days.parquet", index=False)

        top_ticker = (
            ep_wide.groupby(["decision_date", "ticker"], as_index=False)["delta_value_diff"].sum()
            .assign(abs_diff=lambda x: x["delta_value_diff"].abs())
            .sort_values(["decision_date", "abs_diff"], ascending=[True, False])
            .groupby("decision_date")
            .head(5)
        )
        top_ticker.to_parquet(ep_dir / "top_ticker_contributions.parquet", index=False)
        ep_dec.to_parquet(ep_dir / "exp002_decisions_with_ruleflags.parquet", index=False)
        ep_stress.to_parquet(ep_dir / "exp001_stress_with_ruleflags.parquet", index=False)

        start_row = ep_cmp.iloc[0] if len(ep_cmp) else None
        end_row = ep_cmp.iloc[-1] if len(ep_cmp) else None
        if start_row is not None and end_row is not None:
            delta_start = float(start_row["delta_exp002_minus_exp001"])
            delta_end = float(end_row["delta_exp002_minus_exp001"])
            delta_change = delta_end - delta_start
        else:
            delta_start = float("nan")
            delta_end = float("nan")
            delta_change = float("nan")

        report_lines = [
            f"# {eid}",
            "",
            f"- pergunta: {ep['question']}",
            f"- intervalo: `{d0.date()}..{d1.date()}`",
            f"- variante EXP_002 explicada: `{variant_name}_w{variant_w}`",
            "",
            "## Reconciliação e métrica do episódio",
            f"- delta inicial (EXP_002 - EXP_001): `{delta_start:.6f}`",
            f"- delta final (EXP_002 - EXP_001): `{delta_end:.6f}`",
            f"- variação do delta no episódio: `{delta_change:.6f}`",
            f"- max |resíduo identidade| no episódio: `{float(ep_rec['identity_residual'].abs().max() if len(ep_rec) else np.nan):.12f}`",
            f"- max |resíduo delta| no episódio: `{float(ep_rec['residual_delta'].abs().max() if len(ep_rec) else np.nan):.12f}`",
            "",
            "## Dias com maior mudança do delta",
            f"- tabela: `{(ep_dir / 'top_delta_days.parquet').relative_to(working_root)}`",
            "",
            "## Atribuição por ticker (posição/retorno + exposição)",
            f"- tabela: `{(ep_dir / 'top_ticker_contributions.parquet').relative_to(working_root)}`",
            "",
            "## Rule attribution",
            f"- EXP_002 (ruleflags -> score/ação): `{(ep_dir / 'exp002_decisions_with_ruleflags.parquet').relative_to(working_root)}`",
            f"- EXP_001 (ruleflags -> stress sell): `{(ep_dir / 'exp001_stress_with_ruleflags.parquet').relative_to(working_root)}`",
            "",
            "## Fontes usadas",
            f"- `{(shared_tables_dir / 'equity_compare_exp001_vs_exp002.parquet').relative_to(working_root)}`",
            f"- `{(shared_tables_dir / 'equity_reconciliation_daily.parquet').relative_to(working_root)}`",
            f"- `{(shared_tables_dir / 'ticker_contribution_daily.parquet').relative_to(working_root)}`",
            f"- `{(shared_tables_dir / 'exp002_decisions_rule_attribution.parquet').relative_to(working_root)}`",
            f"- `{(shared_tables_dir / 'exp001_stress_rule_attribution.parquet').relative_to(working_root)}`",
            "",
        ]
        write_text(ep_dir / "report.md", "\n".join(report_lines))
        generated_reports.append(str(ep_dir / "report.md"))

    s5_ok = len(generated_reports) == len(episodes)
    gates.append({"name": "S5_EPISODE_REPORTS_WITH_RULE_ATTRIBUTION", "status": "PASS" if s5_ok else "FAIL"})
    steps.append({"name": "S5_EPISODE_REPORTS_WITH_RULE_ATTRIBUTION", "status": "PASS" if s5_ok else "FAIL"})

    # S6 manifest + summary
    files = []
    for p in sorted(outputs_dir.rglob("*")):
        if p.is_file():
            files.append(p)
    hashes = {str(p): sha256_file(p) for p in files}
    shared_manifest = {
        "task_id": task_spec["instruction_id"],
        "generated_at_utc": now_utc(),
        "output_root": str(outputs_dir),
        "required_files": list(hashes.keys()),
        "hashes_sha256": hashes,
        "overall": "PASS",
    }
    write_json(outputs_dir / "shared" / "manifest.json", shared_manifest)
    shared_manifest["hashes_sha256"][str(outputs_dir / "shared" / "manifest.json")] = sha256_file(outputs_dir / "shared" / "manifest.json")
    write_json(outputs_dir / "shared" / "manifest.json", shared_manifest)

    s6_ok = (outputs_dir / "shared" / "manifest.json").exists() and len(files) > 0
    gates.append({"name": "S6_VERIFY_MANIFEST_HASHES_AND_SUMMARY", "status": "PASS" if s6_ok else "FAIL"})
    steps.append({"name": "S6_VERIFY_MANIFEST_HASHES_AND_SUMMARY", "status": "PASS" if s6_ok else "FAIL"})

    overall = all(g["status"] == "PASS" for g in gates)
    run_report = {
        "task_id": task_spec["instruction_id"],
        "status": "PASS" if overall else "FAIL",
        "overall_pass": overall,
        "gates": gates,
        "steps": steps,
        "deliverables": {
            "e1_report": str(outputs_dir / "E1_20191125_20191202" / "report.md"),
            "e2_report": str(outputs_dir / "E2_20200302_20200310" / "report.md"),
            "e3_report": str(outputs_dir / "E3_20210211_20210630" / "report.md"),
            "shared_tables_dir": str(shared_tables_dir),
            "shared_manifest": str(outputs_dir / "shared" / "manifest.json"),
        },
        "timestamp_utc": now_utc(),
    }
    write_json(run_dir / "report.json", run_report)
    write_json(run_dir / "run_summary.json", run_report)
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
