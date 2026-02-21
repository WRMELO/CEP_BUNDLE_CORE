# DEFINICOES CONGELADAS - W1/M3

- task_id: `TASK_CEP_BUNDLE_CORE_ATT2_M3W1_001_LOCATE_W1_M3_SELL_RULES_COST_V1`
- repo_root: `/home/wilson/CEP_BUNDLE_CORE`
- attempt2_root: `/home/wilson/CEP_BUNDLE_CORE/_tentativa2_reexecucao_completa_20260220`
- regra: **sem inferencia** (quando ausente, marcar `AUSENTE`)
- rag_config: `corpus/rag/rag_config.json` (`index_version=v7`)

## 1) Datas exatas de W1

**Status:** `FOUND`

- inicio: `2018-07-02`
- fim: `2021-06-30`

Evidencias:
- `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/metrics_m3_w1_w2_v5.json` (`#L80-L81`, `anchor_type=range`)
  - trecho: `"start_date": "2018-07-02", "end_date": "2021-06-30"`
- `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/metrics_m3_w1_w2_v5.json` (`#L108-L109`, `anchor_type=range`)
  - trecho: `"v3_reference" em W1 repete start/end 2018-07-02..2021-06-30`

## 2) Versao/definicao de M3 valida em W1

**Status:** `PARTIAL_FOUND`

- confirmado: baseline do bundle e `M3 (fixo)`.
- confirmado: formula `score_m3 = z(score_m0) + z(ret_lookback_62) - z(vol_lookback_62)`.
- nao encontrado literalmente: `CEP puro 2 criterios vs ajuste` -> `AUSENTE`.

Evidencia:
- `_tentativa2_reexecucao_completa_20260220/ssot_snapshot/docs/MASTERPLAN_V2.md` (`#L25-L27`, `anchor_type=range`)
  - trecho: `Baseline do bundle: M3 (fixo) ... formula ... score_m3 ...`

## 3) Regras de venda efetivamente ativas em W1

**Status:** `FOUND`

- regra 1: venda por protecao total via SPC/CEP (deterministica, override).
- regra 2: venda complementar via RL para ajuste de risco/rotatividade.
- restricao: regime com `turnover cap`, incluindo `W1=0.22`.

Evidencias:
- `_tentativa2_reexecucao_completa_20260220/ssot_snapshot/docs/MASTERPLAN_V2.md` (`#L102-L103`, `anchor_type=range`)
  - trecho: `Camada 1 ... protecao total ... Camada 2 ... venda complementar ...`
- `_tentativa2_reexecucao_completa_20260220/ssot_snapshot/docs/MASTERPLAN_V2.md` (`#L94`, `anchor_type=line`)
  - trecho: `Venda por protecao total ... override deterministico.`
- `_tentativa2_reexecucao_completa_20260220/ssot_snapshot/docs/MASTERPLAN_V2.md` (`#L79`, `anchor_type=line`)
  - trecho: `Teto de turnover por regime: W1=0.22 ...`

## 4) Custo 0,025% ativo em W1

**Status:** `FOUND`

- modelo: `ARB_COST_0_025PCT_MOVED`
- taxa: `0.00025` (`0,025%`)
- ativo em W1: `sim` (evidencia em metricas W1)

Evidencias:
- `_tentativa2_reexecucao_completa_20260220/ssot_snapshot/docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md` (`#L31-L35`, `anchor_type=range`)
  - trecho: `nome ARB_COST_0_025PCT_MOVED ... taxa 0.00025 ... escopo M3/W1/W2`
- `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/metrics_m3_w1_w2_v5.json` (`#L95-L100`, `anchor_type=range`)
  - trecho: `W1: cost_model ARB_COST_0_025PCT_MOVED ... cost_rate 0.00025`
- `_tentativa2_reexecucao_completa_20260220/ssot_snapshot/docs/MASTERPLAN_V2.md` (`#L104`, `anchor_type=line`)
  - trecho: `Custos: aplicar 0,025% no valor de cada SELL ...`

## Consultas RAG usadas

- dump completo: `_tentativa2_reexecucao_completa_20260220/outputs/M3_W1/locate_definitions_evidence/rag_queries_dump.json`
- consultas executadas:
  - `collection=general` query `W1 inicio fim datas`
  - `collection=general` query `M3 baseline score_m3 definicao`
  - `collection=general` query `regra venda SELL protecao total complementar W1`
  - `collection=lessons` query `custo 0,025% W1`

## Observacao de conformidade

Nao foi alterado nenhum arquivo do snapshot; somente artefatos em:
- `_tentativa2_reexecucao_completa_20260220/work/M3_W1/`
- `_tentativa2_reexecucao_completa_20260220/outputs/M3_W1/`
