# Task 006 - Pos-task (fechamento)

## 0. Identificacao

- LLM autor: `[LLM: GPT 5.3 Codex]`
- Data/hora: 2026-02-11
- Task ID: `TASK_CEP_COMPRA_006_PLOTLY_EQUITY_CASH_VS_CDI_VS_SP500`
- Executor: GPT 5.3 Codex

## 1. OVERALL PASS/FAIL

- **OVERALL: PASS**

## 2. Fonte exata do CDI e do S&P 500

- CDI:
  - Fonte: Banco Central SGS
  - Serie: `12`
  - URL usada: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=02/07/2018&dataFinal=31/12/2025`
  - Regra de indice: `cdi_factor = 1 + taxa_pct/100`; `cdi_index = cumprod(cdi_factor)`
- S&P 500:
  - Fonte: Stooq (fallback web confiavel)
  - Simbolo: `^spx`
  - URL usada: `https://stooq.com/q/d/l/?s=%5Espx&i=d`
  - Regra de indice: `sp500_index = close / close_inicial`

## 3. Serie de carteira_total (auditoria)

- Input principal: `/home/wilson/CEP_COMPRA/outputs/reports/task_005/run_20260211_211715/data/portfolio_daily.parquet`
- Mapeamento de colunas:
  - `date`, `equity_brl`, `cash_brl`, `position_value_brl`
- Checagem:
  - `equity_brl` vs `sum(position_value_brl) + cash_brl`
  - max abs diff: `5.820766091346741e-11`
- Conclusao:
  - `equity_brl` ja inclui caixa; `carteira_total_brl = equity_brl`.

## 4. Regra de alinhamento/forward-fill

- Calendario de referencia: serie da carteira.
- Benchmarks (CDI e S&P 500): alinhados por `date` com `forward-fill`.
- `backward-fill` aplicado apenas se necessario no inicio para evitar nulos lideres.
- Normalizacao final base=1.0 no primeiro dia da carteira no range.

## 5. Paths dos outputs

Run:
- `/home/wilson/CEP_COMPRA/outputs/plots/task_006/run_20260211_213227`

Principais:
- HTML Plotly:
  - `/home/wilson/CEP_COMPRA/outputs/plots/task_006/run_20260211_213227/carteira_vs_cdi_vs_sp500.html`
- Parquet alinhado:
  - `/home/wilson/CEP_COMPRA/outputs/plots/task_006/run_20260211_213227/data/carteira_cdi_sp500_alinhado.parquet`
- Parquets auxiliares:
  - `.../data/carteira_total_base.parquet`
  - `.../data/cdi_raw_index.parquet`
  - `.../data/sp500_raw_index.parquet`
- Governanca:
  - `.../manifest.json`
  - `.../hashes.json`

## 6. Evidencias

- Hashes detalhados:
  - `docs/task_006_evidencias_hashes.md`
