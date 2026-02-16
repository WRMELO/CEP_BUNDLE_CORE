# Report - Sim Compare M3 Plotly

- task_id: `TASK_CEP_BUNDLE_CORE_F1_012_SIM_COMPARE_M3_PLOTLY_V1`
- generated_at_utc: `2026-02-16T13:12:59.325285+00:00`
- branch: `local/integrated-state-20260215`
- baseline_m3_dir_readonly: `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5`
- overall: `PASS`

## Gates

- S1_GATE_REPO_CLEAN: PASS
- S2_GATE_LOAD_M3_ARTIFACTS: PASS
- S3_GATE_RUN_NEW_SIMULATION: PASS
- S4_GATE_COMPARE_METRICS: PASS
- S5_GATE_PLOTLY_HTML: PASS
- S6_GATE_MANIFEST_AND_REPORT: PASS

## Outputs

- `outputs/simulacoes/compare_m3_plotly/20260216/series_new.parquet`
- `outputs/simulacoes/compare_m3_plotly/20260216/metrics_new.json`
- `outputs/simulacoes/compare_m3_plotly/20260216/metrics_compare.csv`
- `outputs/simulacoes/compare_m3_plotly/20260216/equity_curve_compare.html`
- `outputs/simulacoes/compare_m3_plotly/20260216/drawdown_compare.html`
- `outputs/simulacoes/compare_m3_plotly/20260216/manifest.json`
- `outputs/simulacoes/compare_m3_plotly/20260216/evidence/`

## Metrics compare (preview CSV)

```csv
metric,M3,NEW,delta_abs,delta_pct
M3_full.equity_final,2.38543962694251,2.477425086446849,0.0919854595043392,3.8561218848468513
M3_full.multiple,2.3854396269425098,2.477425086446849,0.0919854595043392,3.8561218848468526
M3_full.return_pct,1.3854396269425098,1.477425086446849,0.0919854595043392,6.6394419298760425
M3_full.MDD,-0.6803348624996948,-0.6754282875667084,0.0049065749329864,0.7211999859831685
M3_full.cost_total_recomputed_daily,0.0862314882921513,0.077713197647959,-0.0085182906441922,-9.87839919372884
M3_full.cash_cdi_total_gain_por_periodo,0.1777506358940563,0.1777506358940296,-2.6728619317850644e-14,-1.503714413363986e-11
W1.equity_final,5.369645122088539,5.38447726027694,0.0148321381884013,0.2762219448616437
W1.multiple,5.369645122088538,5.384477260276939,0.0148321381884013,0.2762219448616437
W1.return_pct,4.369645122088538,4.384477260276939,0.0148321381884013,0.3394357613487866
W1.MDD,-0.1873722579559906,-0.1871708317645175,0.0002014261914731,0.1075005412596511
W1.cost_total_recomputed_daily,0.0389218642537312,0.0389218642537254,-5.7523430463390915e-15,-1.4779207411134343e-11
W1.cash_cdi_total_gain_por_periodo,0.0408189233895698,0.0408189233895581,-1.1685097334179771e-14,-2.8626667153023396e-11
```

## Evidence

- `outputs/simulacoes/compare_m3_plotly/20260216/evidence/m3_dir_listing.txt`
- `outputs/simulacoes/compare_m3_plotly/20260216/evidence/gate_checks.json`
- `outputs/simulacoes/compare_m3_plotly/20260216/evidence/metrics_keyset_check.json`
- `outputs/simulacoes/compare_m3_plotly/20260216/evidence/baseline_immutability_check.json`
