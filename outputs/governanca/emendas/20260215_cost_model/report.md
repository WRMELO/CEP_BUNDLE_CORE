# Report - Emenda custo arbitrado 0,025%

- task_id: `TASK_CEP_BUNDLE_CORE_F1_005_EMENDA_COST_MODEL_0025PCT_V1`
- generated_at_utc: `2026-02-15T20:55:42.187454+00:00`
- worktree: `/home/wilson/_wt/CEP_BUNDLE_CORE/bootstrap`
- branch: `wt/bootstrap`
- head_before_commit: `1ab8d76a797d0d0589c4f13779e6dddf7d3aad00`

## Resultado da execucao

Emenda criada em `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md` com regra normativa:

- nome: `ARB_COST_0_025PCT_MOVED`
- taxa: `0.00025` (0,025% / 2,5 bps)
- base: `valor_movimentado_notional`
- formula: `custo_operacao = abs(notional_movimentado) * 0.00025`
- escopo: analises e simulacoes M3/W1/W2 e futuras, ate nova definicao
- vigencia: `2026-02-15`

## Motivacao e impacto

Motivacao registrada: **REGRA AUSENTE** para custo operacional em metricas/controladores.

Impacto: padroniza comparabilidade entre experimentos, instrumentacao e controladores; reduz ambiguidade metodologica.

## Artefatos afetados referenciados

- `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis/outputs/instrumentation/m3_w1_w2/20260215/turnover_costs_derivation.json`
- `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis/outputs/instrumentation/m3_w1_w2/20260215/metrics_m3_w1_w2_v2.json`

Hashes em: `outputs/governanca/emendas/20260215_cost_model/evidence/affected_artifacts_hashes.txt`.

## Evidencias

- `outputs/governanca/emendas/20260215_cost_model/evidence/status_before.txt`
- `outputs/governanca/emendas/20260215_cost_model/evidence/affected_artifacts_hashes.txt`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_WORKTREE_BOOTSTRAP: PASS
- S3_VERIFY_EMENDA_CREATED: PASS
- S4_VERIFY_EMENDA_CONTAINS_RULE: PASS
- S5_WRITE_MANIFEST_HASHES: PASS
- S6_VERIFY_WORKTREE_CLEAN: PENDING (apos commit/sync)

OVERALL (pre-commit): PASS
