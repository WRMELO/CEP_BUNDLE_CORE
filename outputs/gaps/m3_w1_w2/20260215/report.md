# Report - Fechamento de gaps quantitativos M3/W1/W2

- task_id: `TASK_CEP_BUNDLE_CORE_F1_003_CLOSE_GAPS_M3_W1_W2_QUANT_V1`
- generated_at_utc: `2026-02-15T19:25:55.250575+00:00`
- worktree_target: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis`
- branch_target: `wt/analysis-gaps`

## Escopo executado

Foram localizadas fontes legadas reprodutiveis para M3/W1/W2, com hash SHA-256 e recomputacao de metricas a partir de parquet de carteira M3.

## Fontes SSOT selecionadas

- `outputs/reports/task_017/run_20260212_125255/data/daily_portfolio_m3.parquet`
- `outputs/reports/task_017/run_20260212_125255/data/ledger_trades_m3.parquet`
- `outputs/reports/task_017/run_20260212_125255/data/master_state_daily.parquet`
- `outputs/reports/task_018/run_20260212_134037/analise_consolidada_fases_m0_m1_m3.md`
- `outputs/cycle2/20260213/master_regime_m3_cep_only_v3/windows_m3_w1_w2.json`

Detalhamento com hashes: `outputs/gaps/m3_w1_w2/20260215/sources_m3_w1_w2.json`.

## Metricas confirmadas

As metricas foram recomputadas com base em equity diario M3 (`daily_portfolio_m3.parquet`):

- M3_full: `multiple`, `MDD`, `equity_final`, `tempo_em_drawdown_dias`, `tempo_recuperacao_max_dias`.
- W1 (2018-07-02..2021-06-30): `multiple`, `MDD`, `equity_final`, `tempo_em_drawdown_dias`, `tempo_recuperacao_max_dias`.
- W2 (2021-07-01..2022-12-30): `multiple`, `MDD`, `equity_final`, `tempo_em_drawdown_dias`, `tempo_recuperacao_max_dias`.

Arquivo: `outputs/gaps/m3_w1_w2/20260215/metrics_m3_w1_w2.json`.

## Curvas e visualizacoes

Gerado Plotly HTML com:

- equity full + destaque de segmentos W1/W2;
- drawdown e underwater;
- sinais SPC/CEP agregados quando disponiveis em `master_state_daily.parquet`.

Arquivo: `outputs/gaps/m3_w1_w2/20260215/curves/plots_m3_w1_w2.html`.

## Lacunas remanescentes

- `custo_total` e `turnover` dependem da presenca explicita dessas colunas em `ledger_trades_m3.parquet`; quando ausentes, sao registrados como `null`.
- A comparacao M0/M1 em W1/W2 foi mantida como referencia textual do task_018; este pacote recomputa de forma direta apenas a serie M3 e seus cortes W1/W2.

## Reproducao

Comando principal usado:

- `"/home/wilson/PortfolioZero/.venv/bin/python" <script de extracao/recompute desta task>`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_WORKTREE_CREATED_AND_CHECKED_OUT: PASS
- S3_VERIFY_SOURCES_IDENTIFIED_AND_HASHED: PASS
- S4_VERIFY_MINIMUM_METRICS_PRESENT: PASS
- S5_VERIFY_PLOTLY_CURVES_GENERATED: PASS
- S6_VERIFY_REPORT_HAS_TRACEABILITY: PASS
- S7_WRITE_MANIFEST_HASHES: PASS
- S8_VERIFY_WORKTREE_CLEAN: PENDING (validado apos commit/sync)

OVERALL (pre-commit): PASS

## Fechamento de sync e estado final

- commit_head: `04270fdda28698de969690c3d446d160afd9ff1d`
- branch: `wt/analysis-gaps`
- sync evidencias:
  - `outputs/gaps/m3_w1_w2/20260215/evidence/sync_fetch.txt`
  - `outputs/gaps/m3_w1_w2/20260215/evidence/sync_pull_ff_only.txt`
  - `outputs/gaps/m3_w1_w2/20260215/evidence/sync_push.txt`
- gate S8 (worktree clean): PASS
- OVERALL: **PASS**
