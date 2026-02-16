# Report - Build SSOT CDI Daily Series

- task_id: `TASK_CEP_BUNDLE_CORE_F1_011_BUILD_SSOT_CDI_DAILY_SERIES_V1`
- generated_at_utc: `2026-02-16T00:24:12.156040+00:00`
- worktree: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis`
- branch: `wt/analysis-gaps`
- head_before_commit: `d1883a64c079e140695cf8aa68f1256421a89a70`

## Estrategia executada

Foi identificado SSOT reutilizavel em legado/bundle e realizada canonizacao para:

- `data/ssot/cdi/cdi_daily.parquet`

## Fonte e proveniencia

- fonte: `/home/wilson/CEP_COMPRA/outputs/backtests/task_012/run_20260212_114129/consolidated/series_alinhadas_plot.parquet`
- sha256_fonte: `0f4bf81c90cb0d7e4bf9ded96ef3d6cf3e558ed07f8f123deb9277ed4175db7d`
- sha256_ssot: `bee97e79e0654ed79f8846558ffab359087080bd84e70d502dd219fee4eab5da`
- evidencia: `outputs/governanca/ssot_cdi/20260215/evidence/source_provenance.txt`

## Cobertura de datas

- alvo requerido: `2018-07-01` .. `2026-02-15`
- cobertura efetiva da fonte canonizada: `2018-07-02` .. `2025-12-30`
- coverage_start_ok: `False`
- coverage_end_ok: `False`

## Artefatos

- `data/ssot/cdi/cdi_daily.parquet`
- `data/ssot/cdi/README.md`
- `outputs/governanca/ssot_cdi/20260215/report.md`
- `outputs/governanca/ssot_cdi/20260215/manifest.json`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_ANALYSIS_WORKTREE_AND_BRANCH: PASS
- S3_VERIFY_SEARCH_EVIDENCE_WRITTEN: PASS
- S4_VERIFY_SSOT_PARQUET_CREATED: PASS
- S5_VERIFY_README_CREATED_WITH_PROVENANCE: PASS
- S6_WRITE_MANIFEST_HASHES: PASS
- S7_VERIFY_WORKTREE_CLEAN: PENDING (apos commit/sync)

OVERALL (pre-commit): PASS

## Fechamento de sync e estado final

- head_after_commit: `c4a59e7b6f67757863058666228b9919dfb77355`
- branch: `wt/analysis-gaps`
- sync evidencias:
  - `outputs/governanca/ssot_cdi/20260215/evidence/sync_fetch.txt`
  - `outputs/governanca/ssot_cdi/20260215/evidence/sync_pull_ff_only.txt`
  - `outputs/governanca/ssot_cdi/20260215/evidence/sync_push.txt`
  - `outputs/governanca/ssot_cdi/20260215/evidence/status_after_sync.txt`
- gate S7 (worktree clean): PASS
- OVERALL: **PASS**
