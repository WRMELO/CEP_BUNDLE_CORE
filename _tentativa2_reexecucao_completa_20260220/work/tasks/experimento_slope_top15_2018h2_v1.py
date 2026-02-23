from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


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


def slope_ols(y: np.ndarray) -> float:
    n = y.shape[0]
    if n < 2:
        return 0.0
    x = np.arange(n, dtype=float)
    xm = x.mean()
    ym = y.mean()
    den = float(np.sum((x - xm) ** 2))
    if den == 0.0:
        return 0.0
    num = float(np.sum((x - xm) * (y - ym)))
    return num / den


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
    visual_dir = out_root / "visual"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    visual_dir.mkdir(parents=True, exist_ok=True)

    points_path = Path(spec["inputs"]["points_cumulative_daily_parquet"]).resolve()
    standings_path = Path(spec["inputs"]["final_standings_parquet"]).resolve()
    checklist_path = Path(spec["inputs"]["checklist_path"]).resolve()
    cfg = spec["inputs"]["config"]
    top_n = int(cfg["top_n"])
    windows = [int(w) for w in cfg["windows"]]

    generated_at = now_utc()
    gates: dict[str, dict[str, Any]] = {}

    # S1
    checklist_hash_before = sha256_file(checklist_path) if checklist_path.exists() else None
    s1_ok = (
        str(points_path).startswith(str(attempt2_root))
        and str(standings_path).startswith(str(attempt2_root))
        and str(out_root).startswith(str(attempt2_root / "outputs"))
    )
    gates["S1_GATE_ALLOWLIST"] = {"pass": s1_ok}

    # S2
    import subprocess

    comp = subprocess.run(
        [OFFICIAL_PYTHON, "-m", "py_compile", str(Path(__file__).resolve())],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )
    s2_ok = comp.returncode == 0
    gates["S2_CHECK_COMPILE"] = {"pass": s2_ok}

    run_ok = False
    run_error = ""
    try:
        standings = pd.read_parquet(standings_path)
        standings = standings.sort_values(["points_total", "ticker"], ascending=[False, True]).reset_index(drop=True)
        top_tickers = standings.head(top_n)["ticker"].tolist()

        pc = pd.read_parquet(points_path)
        pc["decision_date"] = pd.to_datetime(pc["decision_date"]).dt.normalize()
        pc = pc[pc["ticker"].isin(top_tickers)].copy()
        pc = pc.sort_values(["ticker", "decision_date"]).reset_index(drop=True)
        season_last = pd.Timestamp(pc["decision_date"].max()).normalize()

        rows = []
        for ticker in top_tickers:
            g = pc[pc["ticker"] == ticker].copy().sort_values("decision_date")
            rec = {"ticker": ticker}
            for w in windows:
                gy = g.tail(w)
                slope = slope_ols(gy["points_cumulative"].to_numpy(dtype=float))
                rec[f"slope_{w}"] = float(slope)
            rows.append(rec)

        slopes = pd.DataFrame(rows)
        slopes = slopes.merge(
            standings[["ticker", "points_total"]],
            on="ticker",
            how="left",
        )
        slopes = slopes.sort_values(["points_total", "ticker"], ascending=[False, True]).reset_index(drop=True)
        slopes_path = artifacts_dir / "slopes_top15.parquet"
        slopes[["ticker", "slope_30", "slope_45", "slope_60"]].to_parquet(slopes_path, index=False)

        # Step 6 plotly (3 graficos de linhas no mesmo canvas e mesma escala de Y)
        titles = [f"Slope últimos {w} dias" for w in windows]
        fig = make_subplots(rows=1, cols=3, subplot_titles=titles, horizontal_spacing=0.08, shared_yaxes=True)
        y_cols = [f"slope_{w}" for w in windows]
        y_min = float(slopes[y_cols].min().min())
        y_max = float(slopes[y_cols].max().max())
        pad = (y_max - y_min) * 0.05 if y_max > y_min else 0.1
        y_range = [y_min - pad, y_max + pad]
        for i, w in enumerate(windows, start=1):
            # Mantem a mesma ordem de tickers (standing final) em todos os subplots para comparacao visual direta.
            d = slopes[["ticker", f"slope_{w}"]].rename(columns={f"slope_{w}": "slope"}).reset_index(drop=True)
            fig.add_trace(
                go.Scatter(
                    x=d["ticker"],
                    y=d["slope"],
                    mode="lines+markers",
                    name=f"w{w}",
                    showlegend=False,
                ),
                row=1,
                col=i,
            )
            fig.update_yaxes(title_text="slope", range=y_range, row=1, col=i)
            fig.update_xaxes(title_text="ticker", tickangle=-45, row=1, col=i)
        fig.update_layout(
            title=f"Slope de pontos acumulados (Top {top_n}) - season end {season_last.date()}",
            height=700,
            width=1500,
        )
        html_path = visual_dir / "plotly_slopes_top15_2018H2.html"
        write_text(
            html_path,
            "<html><head><meta charset='utf-8'></head><body>\n"
            + fig.to_html(include_plotlyjs="cdn", full_html=False)
            + "\n</body></html>",
        )

        # Step 8 report
        rep = [
            "# Report - Experimento Slope Top15 2018H2 V1",
            "",
            f"- generated_at_utc: `{generated_at}`",
            f"- top_n: `{top_n}`",
            f"- windows: `{windows}`",
            f"- season_last_decision_date: `{season_last.date()}`",
            "",
            "## Ordenacao por slope_30 (desc)",
        ]
        s30 = slopes[["ticker", "slope_30"]].sort_values(["slope_30", "ticker"], ascending=[False, True])
        for i, (_, r) in enumerate(s30.iterrows(), start=1):
            rep.append(f"{i:02d}. {r['ticker']} - {float(r['slope_30']):.6f}")
        rep.append("")
        rep.append("## Ordenacao por slope_45 (desc)")
        s45 = slopes[["ticker", "slope_45"]].sort_values(["slope_45", "ticker"], ascending=[False, True])
        for i, (_, r) in enumerate(s45.iterrows(), start=1):
            rep.append(f"{i:02d}. {r['ticker']} - {float(r['slope_45']):.6f}")
        rep.append("")
        rep.append("## Ordenacao por slope_60 (desc)")
        s60 = slopes[["ticker", "slope_60"]].sort_values(["slope_60", "ticker"], ascending=[False, True])
        for i, (_, r) in enumerate(s60.iterrows(), start=1):
            rep.append(f"{i:02d}. {r['ticker']} - {float(r['slope_60']):.6f}")
        write_text(out_root / "report.md", "\n".join(rep) + "\n")

        run_ok = True
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        write_text(out_root / "run_exception.txt", run_error + "\n")
        run_ok = False

    gates["S3_RUN"] = {"pass": run_ok, "error": None if run_ok else run_error}

    required_abs = [out_root / rel for rel in spec["outputs"]["required_files"]]
    manifest_path = out_root / "manifest.json"
    write_json(manifest_path, {"task_id": spec["task_id"], "status": "provisional"})

    s4_ok = False
    if run_ok:
        slopes_ok = (artifacts_dir / "slopes_top15.parquet").exists() and len(pd.read_parquet(artifacts_dir / "slopes_top15.parquet")) > 0
        html_ok = html_path.exists() and html_path.stat().st_size > 0
        s4_ok = bool(slopes_ok and html_ok and all(p.exists() for p in required_abs))
    gates["S4_VERIFY_OUTPUTS"] = {"pass": s4_ok}

    hashes: dict[str, str] = {}
    for p in required_abs:
        if p.exists() and p.is_file() and p.name != "manifest.json":
            hashes[str(p)] = sha256_file(p)
    checklist_hash_after = sha256_file(checklist_path) if checklist_path.exists() else None
    checklist_unchanged = checklist_hash_before == checklist_hash_after
    s5_ok = all(p.exists() for p in required_abs) and len(hashes) >= 3 and checklist_unchanged
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

