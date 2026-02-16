# LOCAL DOCUMENT INDEX - CEP_BUNDLE_CORE

- atualizado_em_utc: `2026-02-16T12:39:57.348897+00:00`
- local_root_rel: `.`
- local_root_abs: `/home/wilson/CEP_BUNDLE_CORE`
- branch_integracao: `local/integrated-state-20260215`
- head_integracao: `5ccda0ec24599e0dfa6512344ad1b5e76fc3bdfe`

## Ordem recomendada de leitura (fase fechada)

| Ordem | Bloco | Path relativo | Descricao |
|---|---|---|---|
| 1 | Normas | `docs/CONSTITUICAO.md` | Constituicao (SSOT normativo base). |
| 2 | Normas | `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md` | Emenda de custo operacional 0.025%. |
| 3 | Normas | `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md` | Emenda de remuneracao CDI para caixa. |
| 4 | Normas | `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md` | Emenda de operacao local (LOCAL_ROOT_ONLY). |
| 5 | Plano | `docs/MASTERPLAN.md` | Plano diretor do bundle. |
| 6 | Plano executivo | `docs/MP001_VISAO_EXECUTIVA.md` | Visao executiva canonica. |
| 7 | Plano executivo | `docs/MP002_OBJETIVO_REAL_SSOT.md` | Objetivo real SSOT operacional. |
| 8 | Controle | `outputs/controle/anti_deriva_w2/20260216/report.md` | Pacote anti-deriva W2. |
| 9 | Controle | `outputs/governanca/policy_spc_rl/20260216/report.md` | Politica SPC/CEP e RL subordinado. |
| 10 | Metricas | `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/report.md` | Metricas v5 com CDI SSOT. |
| 11 | Experimentos | `outputs/experimentos/ablation_e1_e5/20260216/report.md` | Ablation E1..E5. |
| 12 | Auditoria | `outputs/governanca/masterplan_audit/20260216_post_sprint_003/report.md` | Prova de 12/12 PASS. |

## Pacote de transferencia V2

- `docs/SESSION_STATE_TRANSFER_PACKAGE_V2.md`
- `docs/SESSION_STATE_TRANSFER_PACKAGE_V2.json`
- `outputs/governanca/phase_closeout/20260216/phase_inventory.csv`

## Referencias de prova 12/12 PASS

- `outputs/governanca/masterplan_audit/20260216_post_sprint_003/masterplan_to_tasks_map.csv`
- `outputs/governanca/masterplan_audit/20260216_post_sprint_003/gaps_and_backlog.json`
- `outputs/governanca/masterplan_audit/20260216_post_sprint_003/report.md`

## Modo operacional padrao

- `LOCAL_ROOT_ONLY` com `planning/task_specs/` e `planning/runs/`.
- Nao depender de `/home/wilson/_wt/*` para operacao normal.
