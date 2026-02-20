# SESSION STATE TRANSFER PACKAGE V3

- generated_at_utc: `2026-02-17T00:00:00Z`
- repo_root: `/home/wilson/CEP_BUNDLE_CORE`
- branch_expected: `local/integrated-state-20260215`
- foco: transferir estado operacional do Masterplan V2 para novo chat, com destaque para a divergencia entre PASS de gate e sinal visual/economico.

## 1) Resumo executivo (F1_001..F2_004)

| Task | Status reportado | Finalidade | Output principal |
|---|---|---|---|
| `F1_001` | PASS | Integridade contabil base (custo/CDI/T+0/caixa/cadencia/reconciliacao) | `outputs/masterplan_v2/f1_001/report.md` |
| `F1_002` | PASS | Decomposicao contabil + Plotly baseline | `outputs/masterplan_v2/f1_002/report.md` |
| `F2_001` | PASS | Envelope continuo e guardrails | `outputs/masterplan_v2/f2_001/envelope_daily.csv` |
| `F2_002` | PASS | Enforcement de executor (caixa/cadencia/guardrails) | `outputs/masterplan_v2/f2_002/report.md` |
| `F2_003` | PASS (contestada) | Auditoria Plotly envelope/guardrails + baseline vs V2 | `outputs/masterplan_v2/f2_003/report.md` |
| `F2_004` | PASS (gate atual) | Sanidade CDI/equity/reconciliacoes para bloquear F3/F4 se falhar | `outputs/masterplan_v2/f2_004/report.md` |

## 2) Outputs e paths criticos por task

- `F1_001`:
  - `outputs/masterplan_v2/f1_001/report.md`
  - `outputs/masterplan_v2/f1_001/manifest.json`
  - `outputs/masterplan_v2/f1_001/evidence/`
- `F1_002`:
  - `outputs/masterplan_v2/f1_002/report.md`
  - `outputs/masterplan_v2/f1_002/manifest.json`
  - `outputs/masterplan_v2/f1_002/plots/*.html`
- `F2_001`:
  - `outputs/masterplan_v2/f2_001/envelope_daily.csv`
  - `outputs/masterplan_v2/f2_001/evidence/guardrails_parameters.json`
  - `outputs/masterplan_v2/f2_001/report.md`
- `F2_002`:
  - `outputs/masterplan_v2/f2_002/daily_portfolio_v2.parquet`
  - `outputs/masterplan_v2/f2_002/ledger_trades_v2.parquet`
  - `outputs/masterplan_v2/f2_002/evidence/enforcement_examples.csv`
  - `outputs/masterplan_v2/f2_002/evidence/metrics_compare_baseline_vs_v2.csv`
- `F2_003`:
  - `outputs/masterplan_v2/f2_003/report.md`
  - `outputs/masterplan_v2/f2_003/manifest.json`
  - `outputs/masterplan_v2/f2_003/evidence/equity_compare_summary.json`
  - `outputs/masterplan_v2/f2_003/plots/baseline_vs_v2_equity_timeseries.html`
  - `outputs/masterplan_v2/f2_003/plots/baseline_vs_v2_drawdown_timeseries.html`
- `F2_004`:
  - `outputs/masterplan_v2/f2_004/report.md`
  - `outputs/masterplan_v2/f2_004/manifest.json`
  - `outputs/masterplan_v2/f2_004/evidence/*`
  - `outputs/masterplan_v2/f2_004/plots/baseline_vs_v2_equity_timeseries_recomputed.html`

## 3) Problema aberto (evidencia objetiva)

- Divergencia central:
  - `F2_003` registra `equity_final_baseline ~= 2.3854` e `equity_final_v2 ~= 183.1974`
  - `delta_pct ~= 7579.818954%`
  - fonte: `outputs/masterplan_v2/f2_003/report.md` e `outputs/masterplan_v2/f2_003/evidence/equity_compare_summary.json`
- Sinal visual:
  - `outputs/masterplan_v2/f2_004/plots/baseline_vs_v2_equity_timeseries_recomputed.html`
  - baseline e V2 em escalas muito distantes, com V2 monotonicamente explosivo.
- Conclusao operacional:
  - ha inconsistencia tecnico-economica ainda nao encerrada por governanca robusta.
  - nao inferir intencao; tratar como falha de desenho/forca dos gates e/ou de modelagem contábil.

## 4) Estado da contestacao da F2_004

- `F2_004` esta atualmente com `overall PASS` e `block_f3_f4=false` no report/manifest.
- Mesmo assim, por criterio de prudencia operacional, o status recomendado para planejamento e:
  - `PASS CONTESTADO` ate reforco de criterios de sanidade e prova de coerencia economica.

## 5) Regra de bloqueio para o proximo chat

- Regra proposta:
  - nao avancar para `F3_*` ou `F4_*` sem fechar a divergencia de equity.
  - reexecutar gate forte de sanidade com limites economicos explicitos e objetivos.

## 6) Proximo passo recomendado (correcao minima)

- Abrir task de correcao minima com escopo:
  - reforcar gate de sanidade (bound economico de equity vs benchmark cash-only e vs exposicao media),
  - validar retorno diario implicito e anualizacao aproximada,
  - provar ausencia de dupla contagem de CDI no equity,
  - rerodar `F2_004`; se necessario, corrigir `F2_002/F2_003` e rerodar somente o minimo.

## 7) Comandos minimos para reproducao

- Executar F2_004:
  - `/home/wilson/PortfolioZero/.venv/bin/python scripts/agno_runner.py --task planning/task_specs/masterplan_v2/TASK_CEP_BUNDLE_CORE_V2_F2_004_EQUITY_CDI_SANITY_AND_RECONCILIATION_GATE.json`
- Executar F2_003:
  - `/home/wilson/PortfolioZero/.venv/bin/python scripts/agno_runner.py --task planning/task_specs/masterplan_v2/TASK_CEP_BUNDLE_CORE_V2_F2_003_ENVELOPE_PLOTLY_AUDIT.json`
- Executar F2_002:
  - `/home/wilson/PortfolioZero/.venv/bin/python scripts/agno_runner.py --task planning/task_specs/masterplan_v2/TASK_CEP_BUNDLE_CORE_V2_F2_002_EXECUTOR_CASH_AND_CADENCE_ENFORCEMENT.json`

## 8) Commits do periodo e motivo

- `24d5b6a` - corrigir interpretacao de CDI index-like em F2_004.
- `97abf54` - adicionar gate F2_004 no task tree/spec e suporte no runner.
- `a85cf1e` - corrigir hash de path externo no manifest da F2_003.
- `1d7eb88` - adicionar comparativos equity/drawdown na F2_003.
- `592c118` - adicionar suporte F2_003 no runner.
- `1f7ffd0` - corrigir avaliacao de outputs na F2_002.
- `d0b1a1c` - normalizar escalares numpy para serializacao json.
- `39cda73` - adicionar suporte F2_002 no runner.
- `deb9282` - derivar buy/sell notional via ledger em F2_001.
- `45b0131` - adicionar suporte F2_001 no runner.
- `6d04156` - adicionar suporte F1_002 no runner.
- `f66dc4b` - rastrear `scripts/agno_runner.py`.

## 9) Evidencias de coleta deste pacote

- `outputs/masterplan_v2/_transfer/evidence_outputs_tree.txt`
- `outputs/masterplan_v2/_transfer/evidence_reports_head.txt`
- `outputs/masterplan_v2/_transfer/evidence_git_log_40.txt`

