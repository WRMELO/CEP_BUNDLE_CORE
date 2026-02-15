# Report - Aplicacao do cost model 0,025% (v3)

- task_id: `TASK_CEP_BUNDLE_CORE_F1_006_APPLY_COST_MODEL_TO_W1W2_METRICS_V1`
- generated_at_utc: `2026-02-15T20:58:43.354080+00:00`
- worktree: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis`
- branch: `wt/analysis-gaps`
- head_before_commit: `dcb1985bff58801dff7f0789364a60e4be460452`

## Regra aplicada

Regra normativa aplicada conforme emenda:

- emenda: `/home/wilson/_wt/CEP_BUNDLE_CORE/bootstrap/docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
- modelo: `ARB_COST_0_025PCT_MOVED`
- taxa: `0.00025` (0,025% / 2,5 bps)
- base: `sum(abs(notional_movimentado))` por periodo
- formula: `cost_total_period = cost_rate * valor_movimentado_periodo`

## Base de valor_movimentado usada

Foi utilizada a mesma base de `notional` do ledger usada na derivacao de turnover em v2:

- ledger: `/home/wilson/CEP_COMPRA/outputs/reports/task_017/run_20260212_125255/data/ledger_trades_m3.parquet`
- evidencia de schema/metodologia anterior: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis/outputs/instrumentation/m3_w1_w2/20260215/turnover_costs_derivation.json`

## Resultados por periodo

### M3_full
- valor_movimentado_notional: `344.92595316860525`
- cost_total: `0.08623148829215131`
- multiple bruto -> liquido: `2.3854396269425098` -> `2.2992081386503584`

### W1
- valor_movimentado_notional: `155.68745701492486`
- cost_total: `0.03892186425373122`
- multiple bruto -> liquido: `5.369645122088538` -> `5.330723257834807`

### W2
- valor_movimentado_notional: `102.33749645585225`
- cost_total: `0.02558437411396306`
- multiple bruto -> liquido: `0.4673457809709631` -> `0.4625774658069277`

## Artefatos gerados

- `outputs/instrumentation/m3_w1_w2/20260215_costs_v3/metrics_m3_w1_w2_v3.json`
- `outputs/instrumentation/m3_w1_w2/20260215_costs_v3/turnover_costs_derivation_v3.json`
- `outputs/instrumentation/m3_w1_w2/20260215_costs_v3/manifest.json`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_ANALYSIS_WORKTREE_AND_BRANCH: PASS
- S3_VERIFY_COSTS_COMPUTED: PASS
- S4_VERIFY_METRICS_V3_CREATED: PASS
- S5_VERIFY_REPORT_TRACEABILITY: PASS
- S6_WRITE_MANIFEST_HASHES: PASS
- S7_VERIFY_WORKTREE_CLEAN: PENDING (apos commit/sync)

OVERALL (pre-commit): PASS
