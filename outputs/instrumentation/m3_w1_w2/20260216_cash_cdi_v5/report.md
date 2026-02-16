# Report - Apply CDI SSOT to Metrics V5

- task_id: `TASK_CEP_BUNDLE_CORE_F1_012_APPLY_CDI_SSOT_TO_METRICS_V5`
- generated_at_utc: `2026-02-16T12:05:29.850404+00:00`
- branch: `local/integrated-state-20260215`
- head_before_commit: `40bdf8586b8d18b96020890acda4b633ca026310`

## Ancoragem normativa

- Constituicao: `docs/CONSTITUICAO.md`
- Emenda custo: `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
- Emenda cash CDI: `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
- Masterplan: `docs/MASTERPLAN.md`

## SSOT CDI aplicado

- path: `data/ssot/cdi/cdi_daily.parquet`
- sha256: `bee97e79e0654ed79f8846558ffab359087080bd84e70d502dd219fee4eab5da`
- cobertura: `2018-07-02` .. `2025-12-30`

## Metodologia reproduzivel

1. Carregar serie diaria de equity/cash (`daily_portfolio_m3.parquet`) e ledger (`ledger_trades_m3.parquet`).
2. Derivar custo diario por `sum(abs(notional))*0.00025`.
3. Aplicar ganho CDI do caixa por pregao: `cdi_cash_gain_t = cash_(t-1) * cdi_ret_t`.
4. Replay liquido: `equity_net_t = equity_net_(t-1)*(1+gross_ret_t) + cdi_cash_gain_t - daily_cost_t`.
5. Agregar por periodos M3_full/W1/W2 para emitir v5.

## Fechamento do status CDI

- status_v5: `APPLIED_BY_SSOT`
- cash_cdi_applied: `true`
- evidencia: `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/metrics_m3_w1_w2_v5.json`

## Resultado por periodo (v5)

- M3_full: cash_gain=`0.177751`, multiple_liq_custos_cdi=`2.470239`
- W1: cash_gain=`0.040819`, multiple_liq_custos_cdi=`5.384477`
- W2: cash_gain=`0.063213`, multiple_liq_custos_cdi=`0.473961`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_SSOT_CDI_PRESENT: PASS
- S3_VERIFY_METRICS_V5_EMITTED: PASS
- S4_VERIFY_CASH_CDI_STATUS_CLOSED: PASS
- S5_VERIFY_REPORT_TRACEABLE: PASS
- S6_WRITE_MANIFEST_HASHES: PASS
- S7_VERIFY_REPO_CLEAN_AND_SYNCED: PENDING (apos commit/sync)
