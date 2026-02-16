# SESSION STATE TRANSFER PACKAGE V2

- generated_at_utc: `2026-02-16T12:39:57.348897+00:00`
- repo_root: `/home/wilson/CEP_BUNDLE_CORE`
- branch: `local/integrated-state-20260215`
- head: `5ccda0ec24599e0dfa6512344ad1b5e76fc3bdfe`
- objetivo: congelar estado canonico com Masterplan `12/12 PASS` para continuidade sem regressao.

## 1) Prova objetiva do estado 12/12 PASS

- audit dir: `outputs/governanca/masterplan_audit/20260216_post_sprint_003/`
- map: `outputs/governanca/masterplan_audit/20260216_post_sprint_003/masterplan_to_tasks_map.csv`
- gaps: `outputs/governanca/masterplan_audit/20260216_post_sprint_003/gaps_and_backlog.json`
- report: `outputs/governanca/masterplan_audit/20260216_post_sprint_003/report.md`
- closed_items_count: `12`
- pending_items_count: `0`
- target_12_of_12_pass: `True`

## 2) SSOTs normativos ativos (com hash)

- `docs/CONSTITUICAO.md` | sha256 `01c962bba0979e842589e5f8898422c045e43f29d2f970dbfb58147c28605a63`
- `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md` | sha256 `27447cc52c6c3a46d13c26a1787989c209b93c6a04e5cb59c5c11e1ba6bceed0`
- `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md` | sha256 `c8a4968b07a88e3583ded14fb6bc5987c255dc722307e485b15bda8c5523e8e8`
- `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md` | sha256 `e7a099f3fca1c4249a0baa10c05d71c369f07c9da9fe6adb47e91755b36cf72e`

## 3) Arquitetura de execucao local

- modo padrao: `LOCAL_ROOT_ONLY`
- estruturas obrigatorias:
  - `planning/task_specs/`
  - `planning/runs/`
  - `outputs/governanca/<pacote>/...`
- evidencia de referencia: `docs/EXECUTION_STRUCTURE_LOCAL.md`

## 4) Artefatos-chave para continuidade (com hash)

- `docs/MP001_VISAO_EXECUTIVA.md` | sha256 `716e4391131193a60552e00478c96c2d0780ac544d23ef77a5bd338abf18a440`
- `docs/MP002_OBJETIVO_REAL_SSOT.md` | sha256 `e50124619a94a0524c89b446331a4e88e8e3225fb2a4ba89299ae60255a45219`
- `data/ssot/cdi/cdi_daily.parquet` | sha256 `bee97e79e0654ed79f8846558ffab359087080bd84e70d502dd219fee4eab5da`
- `data/ssot/cdi/README.md` | sha256 `de999f5c680a4c869949a8c890d31e0e126c3e65645c828bc0f62d2b62dcc6fc`
- `outputs/controle/anti_deriva_w2/20260216/report.md` | sha256 `3cb48b385515f03fd23202e3f4a04333ee54f5e52ac834cf7d016915e46c1dc8`
- `outputs/controle/anti_deriva_w2/20260216/anti_deriva_w2_summary.json` | sha256 `2cd7ca475fa188f5c32491bf307f9eed00a9716a0fc411599dde89f2c83c4a46`
- `outputs/governanca/policy_spc_rl/20260216/report.md` | sha256 `6517fbc47ca9d7c6b482e8bc699fa1a6a2abacdabcc1c690f24ac70acfa8db6c`
- `outputs/governanca/policy_spc_rl/20260216/policy_spc_rl_summary.json` | sha256 `e07941bf4301b5d6f59aa0ca279d1ac1e0e4970e85331bf0f8f512d1b623ce4b`
- `outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/report.md` | sha256 `951952286bf309c11dd195ccb216bfb1b9a1436da6bf233efae59a52a0f51c9b`
- `outputs/experimentos/ablation_e1_e5/20260216/report.md` | sha256 `d3c8a2e1571d4606915ab59c8deef8f7ec00322110fcc5bcb84653b637ff55d0`
- `outputs/gaps/g1_g3/20260216/g1_g3_closure.json` | sha256 `310af35ab4c5072d4598d572d919af141ad8fc2b44bdd8bac7c9ae4c1c27e3c0`

## 5) Ordem recomendada de leitura

1. Normas: Constituicao + emendas ativas.
2. Plano: `docs/MASTERPLAN.md`.
3. Executivos: `docs/MP001_VISAO_EXECUTIVA.md` e `docs/MP002_OBJETIVO_REAL_SSOT.md`.
4. Controles: anti-deriva W2 + policy SPC/RL.
5. Metricas e experimentos: metrics v5 + ablation E1..E5.
6. Auditoria final: pacote pos sprint 003 com 12/12 PASS.

## 6) Rotinas minimas anti-regressao

- Sempre abrir task com spec em `planning/task_specs/` e run em `planning/runs/`.
- Exigir em todo ciclo: `report.md`, `manifest.json`, `evidence/`.
- Antes de alterar regra estrutural, validar Constituicao/Emendas e, se preciso, abrir nova emenda.
- Em refresh de Masterplan, atualizar mapa/gaps e comprovar status por evidencia objetiva.
- Fechar ciclo com `git status --porcelain` vazio e push sem force.

## 7) Inventario de fase

- `outputs/governanca/phase_closeout/20260216/phase_inventory.csv`
