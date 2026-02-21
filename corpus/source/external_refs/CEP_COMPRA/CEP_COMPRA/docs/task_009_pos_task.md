# Task 009 - Pos-task (fechamento)

## 0. Identificacao

- LLM autor: `[LLM: GPT 5.3 Codex]`
- Data/hora: 2026-02-12
- Task ID: `TASK_CEP_COMPRA_009_VERIFICAR_SELL_POR_ROMPIMENTO_POSITIVO_E_IMPACTO_PLATO`
- Executor: GPT 5.3 Codex

## 1. OVERALL PASS/FAIL

- **OVERALL: PASS**

## 2. Onde a regra de SELL está definida (evidência)

- Regra de SELL diária aplicada no fluxo da carteira (`CEP_COMPRA`):
  - arquivo: `tools/task_005_report_runner.py`
  - condição: `r < i_lcl` **ou** `r > i_ucl` **ou** `mr > mr_ucl` implica venda da posição (`cash += pos.pop(t)`).
- Regra de estado Master (artefato congelado `CEP_NA_BOLSA`):
  - arquivo: `src/cep/runners/runner_backtest_fixed_limits_exp031_v1.py`
  - sinais: `r > ucl_r`, `xbar > ucl_xbar`, `xt < lcl_i`, `xt > ucl_i`
  - exceção de upside: `upside_extreme = xbar_up | stress_i_up`, com `stress_amp = stress_amp_raw & (~upside_extreme)`.

## 3. Resposta objetiva (UCL positivo em Xbar/R/I/MR)

- Xbar/UCL: **não há SELL direto** no fluxo da carteira; funciona como exceção de upside no Master.
- R/UCL: **há gatilho de estresse no Master** (indireto para defesa/regime), não SELL direto por ticker no runner da carteira.
- I/UCL: **sim**, SELL direto na carteira.
- MR/UCL: **sim**, SELL direto na carteira.

## 4. Quantificação e impacto (2022-07-01..2023-06-30 e foco plateau)

- Intervalo amplo (universo):
  - I/UCL: `11944` eventos
  - I/LCL: `11603` eventos
  - MR/UCL: `33543` eventos
- Intervalo foco plateau (universo):
  - I/UCL: `5382` eventos
  - I/LCL: `5451` eventos
  - MR/UCL: `15821` eventos
- Para tickers efetivamente em carteira no intervalo amplo:
  - `held_rows_total=2237`, `held_rows_xt_missing=1797`, `held_rows_with_full_data=440`
  - violações em carteira com cobertura completa: `i_ucl=0`, `i_lcl=0`, `mr_ucl=0`

Conclusão evidencial sobre o plateau: não há suporte para explicar o plateau por SELL de UCL em Xbar; no recorte de ativos em carteira com dados completos não houve violações I/MR, e a explicação permanece ligada à combinação de regime Master + desempenho dos ativos mantidos.

## 5. Definição de X_t (sem inferência)

- Fonte formal: `CEP_NA_BOLSA_CONSTITUICAO_V2_20260204.md`
- Definição explícita: `X_t = log(Close_t / Close_{t-1})`
- Schema operacional confirma campo `xt` em `base_operacional_xt.csv`.

## 6. Paths finais

- Run oficial: `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212`
- Regras: `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212/regras_sell_xbar_r_i_mr.md`
- Relatório: `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212/relatorio_task_009_plato_e_rompimentos.md`
- Parquets:
  - `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212/data/eventos_rompimento.parquet`
  - `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212/data/eventos_com_sell.parquet`
  - `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212/data/metricas_impacto.parquet`
  - `/home/wilson/CEP_COMPRA/outputs/forensics/task_009/run_20260212_105212/data/held_coverage.parquet`
