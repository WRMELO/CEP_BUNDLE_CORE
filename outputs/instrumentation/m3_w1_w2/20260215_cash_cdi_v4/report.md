# Report - Aplicacao condicionada da emenda CASH_ACCRUES_CDI (v4)

- task_id: `TASK_CEP_BUNDLE_CORE_F1_010_APPLY_CDI_CASH_ACCRUAL_TO_METRICS_V4_V1`
- generated_at_utc: `2026-02-16T00:14:01.070205+00:00`
- worktree: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis`
- branch: `wt/analysis-gaps`
- head_before_commit: `5cdd4c9efbc083436e3bc438e0ce6fce5a1ab730`

## Busca SSOT CDI

- roots varridos: `docs/`, `docs/corpus/`, `outputs/`, `data/`, `planning/`
- keywords: `CDI`, `cdi`, `taxa_cdi`, `serie_cdi`, `cdi_daily`, `cdi_diario`
- resultado: **SSOT AUSENTE**

Evidencia detalhada: `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/evidence/ssot_cdi_search.txt`.

## Decisao operacional

Como SSOT CDI nao foi encontrado de forma confiavel no escopo definido, **nao foi aplicada serie CDI** e nenhuma serie foi inventada.

- `metrics_v4` gerado como extensao de v3, preservando `cost_model` e adicionando `cash_cdi_status='SSOT AUSENTE'`.
- `cash_cdi_derivation_v4.json` gerado com status `SSOT AUSENTE` e metodologia de busca.

## Artefatos gerados

- `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/metrics_m3_w1_w2_v4.json`
- `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/cash_cdi_derivation_v4.json`
- `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/manifest.json`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_ANALYSIS_WORKTREE_AND_BRANCH: PASS
- S3_VERIFY_CDI_SSOT_SEARCH_EVIDENCE: PASS
- S4_VERIFY_METRICS_V4_CREATED: PASS
- S5_VERIFY_REPORT_TRACEABILITY: PASS
- S6_WRITE_MANIFEST_HASHES: PASS
- S7_VERIFY_WORKTREE_CLEAN: PENDING (apos commit/sync)

OVERALL (pre-commit): PASS

## Fechamento de sync e estado final

- head_after_commit: `af741b223a3d99cb854a2f6ca226d6bdd790c4ba`
- branch: `wt/analysis-gaps`
- sync evidencias:
  - `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/evidence/sync_fetch.txt`
  - `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/evidence/sync_pull_ff_only.txt`
  - `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/evidence/sync_push.txt`
  - `outputs/instrumentation/m3_w1_w2/20260215_cash_cdi_v4/evidence/status_after_sync.txt`
- gate S7 (worktree clean): PASS
- OVERALL: **PASS**
