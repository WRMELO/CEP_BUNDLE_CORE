# Report - Anti-deriva W2 Complete

- task_id: `TASK_CEP_BUNDLE_CORE_S2_002_MP007_COMPLETE_ANTI_DERIVA_W2`
- generated_at_utc: `2026-02-16T12:21:36.068794+00:00`

## Selecao objetiva da camada

Selecionado: `E4_ADD_ANTI_REENTRY_FILTER` com delta_multiple_W2 `0.232844` e delta_MDD_W2 `0.229485`.

## Guardrails operacionalizados

- histerese regime: on `-0.2` / off `-0.1`
- cap turnover por regime: `{'W1': 0.22, 'W2': 0.12, 'W3': 0.15, 'OTHER': 0.16}`
- anti-reentry stress multiplier: `0.8`

## Evidencias

- `outputs/experimentos/ablation_e1_e5/20260216/results_summary.json`
- `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/metrics_m3_w1_w2_v5.json`
