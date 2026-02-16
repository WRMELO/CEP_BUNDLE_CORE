# Report - Fix bootstrap clean (CONSTITUICAO)

- task_id: `TASK_CEP_BUNDLE_CORE_F1_009B_FIX_BOOTSTRAP_DIRTY_CONSTITUICAO_V1`
- generated_at_utc: `2026-02-16T00:20:32.578595+00:00`
- worktree: `/home/wilson/_wt/CEP_BUNDLE_CORE/bootstrap`
- branch: `wt/bootstrap`
- head_before: `554e063286010cdafe6c2a9cd3ff50bcf8f05c8e`
- head_after: `554e063286010cdafe6c2a9cd3ff50bcf8f05c8e`

## Acao executada

1. Capturado status inicial em `status_before.txt`.
2. Capturado patch da sujeira preexistente em `CONSTITUICAO_dirty.patch`.
3. Restaurado `docs/CONSTITUICAO.md` para `HEAD` via `git restore --source=HEAD -- docs/CONSTITUICAO.md`.
4. Capturado status final em `status_after.txt`.

## Verificacao objetiva

- tamanho do patch capturado: `466` bytes
- status antes continha modificacao em `docs/CONSTITUICAO.md`: `SIM`
- status final limpo: `SIM`

## Evidencias

- `outputs/governanca/fix_bootstrap_clean/20260215/evidence/CONSTITUICAO_dirty.patch`
- `outputs/governanca/fix_bootstrap_clean/20260215/evidence/status_before.txt`
- `outputs/governanca/fix_bootstrap_clean/20260215/evidence/status_after.txt`
- `outputs/governanca/fix_bootstrap_clean/20260215/evidence/head_before.txt`
- `outputs/governanca/fix_bootstrap_clean/20260215/evidence/head_after.txt`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_WORKTREE_BOOTSTRAP: PASS
- S3_VERIFY_DIRTY_PATCH_CAPTURED: PASS
- S4_VERIFY_FILE_RESTORED_TO_HEAD: PASS
- S5_VERIFY_WORKTREE_CLEAN: PASS
- S6_WRITE_MANIFEST_HASHES: PASS

OVERALL: PASS
