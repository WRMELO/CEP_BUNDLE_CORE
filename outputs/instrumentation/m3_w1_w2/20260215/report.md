# Report - Instrumentacao W1/W2 e custos (M3)

- task_id: `TASK_CEP_BUNDLE_CORE_F1_004_INSTRUMENTATION_W1W2_AND_COSTS_V1`
- generated_at_utc: `2026-02-15T19:38:05.347110+00:00`
- worktree: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis`
- branch: `wt/analysis-gaps`

## Escopo

1. Formalizacao de recortes W1/W2 com datas objetivas e criterio.
2. Derivacao reprodutivel de turnover e num_trades.
3. Revisao de custos com evidencia objetiva de disponibilidade/ausencia de regra SSOT.
4. Atualizacao do pacote de metricas para `v2`.

## Recortes W1/W2 formalizados

Arquivo: `outputs/instrumentation/m3_w1_w2/20260215/w1w2_segments.json`

- W1: `2018-07-02` a `2021-06-30`
- W2: `2021-07-01` a `2022-12-30`

Criterio objetivo:

- regra primaria: janela canonica definida no SSOT do task_018 para comparabilidade historica;
- validacao de curva: transicao apos pico com quebra sustentada de drawdown (dd <= -20% com permanencia em drawdown por >=20 observacoes).

## Turnover e custos

Arquivo: `outputs/instrumentation/m3_w1_w2/20260215/turnover_costs_derivation.json`

Turnover:

- metodo aplicado: `sum(abs(notional) por dia) / equity_do_dia`;
- num_trades derivado de `ledger_trades_m3.parquet` por janela.

Custos:

- colunas diretas de custo nao encontradas no ledger (`cost_total/cost_brl/fee_brl/commission/slippage`);
- regra SSOT de estimativa em `docs/CONSTITUICAO.md`: **REGRA AUSENTE**;
- `cost_total` mantido como `null` com justificativa e evidencia.

## Metricas v2

Arquivo: `outputs/instrumentation/m3_w1_w2/20260215/metrics_m3_w1_w2_v2.json`

Inclui por segmento M3_full/W1/W2:

- `multiple`, `MDD`, `equity_final`;
- `turnover.mean_daily`, `turnover.sum_daily`;
- `num_trades`;
- `cost_total`, `cost_model`, `cost_status`;
- referencias `multiple_v1_reference` e `MDD_v1_reference` para rastreabilidade.

## Artefato de curvas analisado nesta task

- origem: `outputs/gaps/m3_w1_w2/20260215/curves/plots_m3_w1_w2.html`
- copia fixa desta task: `outputs/instrumentation/m3_w1_w2/20260215/curves/plots_m3_w1_w2.html`

## Evidencias

- `outputs/instrumentation/m3_w1_w2/20260215/evidence/source_paths_and_hashes.json`
- `outputs/instrumentation/m3_w1_w2/20260215/evidence/ledger_columns.txt`
- `outputs/instrumentation/m3_w1_w2/20260215/evidence/cost_rule_search_summary.txt`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_ANALYSIS_WORKTREE_AND_BRANCH: PASS
- S3_VERIFY_W1W2_SEGMENTS_PRESENT: PASS
- S4_VERIFY_TURNOVER_DERIVATION_PRESENT: PASS
- S5_VERIFY_COSTS_DERIVATION_PRESENT: PASS
- S6_VERIFY_METRICS_V2_PRESENT_AND_EXTENDS_V1: PASS
- S7_VERIFY_REPORT_TRACEABILITY: PASS
- S8_WRITE_MANIFEST_HASHES: PASS
- S9_VERIFY_WORKTREE_CLEAN: PENDING (apos commit/sync)

OVERALL (pre-commit): PASS

## Fechamento de sync e estado final

- commit_head: `c85793fa497ec5b7b586cdb414149e70116c8441`
- branch: `wt/analysis-gaps`
- sync evidencias:
  - `outputs/instrumentation/m3_w1_w2/20260215/evidence/sync_fetch.txt`
  - `outputs/instrumentation/m3_w1_w2/20260215/evidence/sync_pull_ff_only.txt`
  - `outputs/instrumentation/m3_w1_w2/20260215/evidence/sync_push.txt`
- gate S9 (worktree clean): PASS
- OVERALL: **PASS**
