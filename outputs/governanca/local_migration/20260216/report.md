# Report - Local Migration Worktrees -> LOCAL_ROOT

- task_id: `TASK_CEP_BUNDLE_CORE_F0_013_MIGRATE_WORKTREES_TO_LOCAL_OPERATION_V1`
- generated_at_utc: `2026-02-16T09:55:29.428162+00:00`
- local_root_selected: `/home/wilson/CEP_BUNDLE_CORE`
- integration_branch: `local/integrated-state-20260215`
- head_current: `0e5a122cfebe26feabc5d22f665e3645225e35fd`

## Decisao de LOCAL_ROOT

Foi detectado repositorio local operacional fora de `_wt` em `/home/wilson/CEP_BUNDLE_CORE`; ele foi selecionado como ponto unico de operacao.

## Consolidacao das linhas

Consolidacao realizada nesta branch via merge de:
- `origin/wt/bootstrap`
- `origin/wt/analysis-gaps`

Sem remocao de worktrees nesta task.

## Verificacao do conjunto minimo requerido

Resultado em `evidence/required_artifacts_check.json`.

## Indice humano

Criado: `docs/LOCAL_DOCUMENT_INDEX.md`.

## Bundle externo para leitura

Criado em: `/home/wilson/CEP_BUNDLE_CORE_exports/20260216/`
- `/home/wilson/CEP_BUNDLE_CORE_exports/20260216/bundle_manifest.json`
- `/home/wilson/CEP_BUNDLE_CORE_exports/20260216/bundle_manifest.md`

## Modo operacional LOCAL_ROOT

Ajuste minimo aplicado: orientacao explicita no indice humano para executar proximas tasks em `/home/wilson/CEP_BUNDLE_CORE`.
Nao foi necessario alterar configuracao de runner/allowlist nesta task.

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_LOCAL_ROOT_DECISION_WITH_EVIDENCE: PASS
- S3_VERIFY_REMOTE_BRANCHES_AVAILABLE: PASS
- S4_VERIFY_LOCAL_INTEGRATION_STATE_CREATED: PASS
- S5_VERIFY_REQUIRED_ARTIFACTS_PRESENT_IN_LOCAL_ROOT: PASS
- S6_VERIFY_HUMAN_INDEX_CREATED: PASS
- S7_VERIFY_EXTERNAL_BUNDLE_AND_HASHES: PASS
- S8_VERIFY_OPERATION_MODE_LOCAL_ROOT: PASS
- S9_VERIFY_CLEAN_FINAL_STATE: PASS

## Fechamento de sync

- branch: `local/integrated-state-20260215`
- head_after_sync: `a8742bd8390bc3a25522e96597a7cc6e3a832b1f`
- evidencias:
  - `outputs/governanca/local_migration/20260216/evidence/sync_fetch.txt`
  - `outputs/governanca/local_migration/20260216/evidence/sync_pull_ff_only.txt`
  - `outputs/governanca/local_migration/20260216/evidence/sync_push.txt`
  - `outputs/governanca/local_migration/20260216/evidence/status_after_sync.txt`
- OVERALL: **PASS**
