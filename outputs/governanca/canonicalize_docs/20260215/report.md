# Report - Canonicalizacao de documentos de suporte

- task_id: `TASK_CEP_BUNDLE_CORE_F1_007_CANONICALIZE_SUPPORT_DOCS_FROM_WORKTREES_AND_REMOTE_V1`
- generated_at_utc: `2026-02-15T21:16:14.859240+00:00`
- repo_root: `/home/wilson/CEP_BUNDLE_CORE`
- branch: `main`
- head_before_commit: `4c84a020fa947401f26f5fd98c551e5f4ae7d59c`

## Escopos varridos

- worktrees locais: `current + all worktrees`
- branches locais: `git show / git ls-tree`
- branches remotas: `git show / git ls-tree`

## Resultado

- total_arquivos_copiados_para_canonico: **12**
- diretorio canonico: `docs/_evidencias/20260215_support_docs/`
- indices gerados:
  - `docs/_evidencias/20260215_support_docs/INDEX.md`
  - `docs/_evidencias/20260215_support_docs/index.json`

## Evidencias

- `outputs/governanca/canonicalize_docs/20260215/evidence/worktree_list.txt`
- `outputs/governanca/canonicalize_docs/20260215/evidence/branches_local.txt`
- `outputs/governanca/canonicalize_docs/20260215/evidence/branches_remote.txt`
- `outputs/governanca/canonicalize_docs/20260215/evidence/search_log.json`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_REPO_AND_GIT_OK: PASS
- S3_VERIFY_SEARCH_COMPLETED_ALL_SCOPES: PASS
- S4_VERIFY_AT_LEAST_ONE_TARGET_FOUND: PASS
- S5_VERIFY_CANONICAL_DIR_POPULATED: PASS
- S6_VERIFY_INDEX_CREATED: PASS
- S7_WRITE_MANIFEST_HASHES: PASS
- S8_VERIFY_WORKTREE_CLEAN: PENDING (apos commit/sync)

OVERALL (pre-commit): PASS

## Fechamento de sync e estado final

- head_after_commit: `95d97c23fb07858fc535b5f359d9fb9a69aded8f`
- branch: `main`
- sync evidencias:
  - `outputs/governanca/canonicalize_docs/20260215/evidence/sync_fetch.txt`
  - `outputs/governanca/canonicalize_docs/20260215/evidence/sync_pull_ff_only.txt`
  - `outputs/governanca/canonicalize_docs/20260215/evidence/sync_push.txt`
  - `outputs/governanca/canonicalize_docs/20260215/evidence/status_after_sync.txt`
- gate S8 (worktree clean): PASS
- OVERALL: **PASS**
