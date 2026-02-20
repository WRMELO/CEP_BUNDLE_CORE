# EMENDA CONSTITUCIONAL - COST MODEL ARBITRADO 0,025%

## Metadados

- emenda_id: E-2026-02-15-COST-MODEL-ARB-0025PCT-V1
- versao_emenda: V1
- status: aprovada
- data_proposta: 2026-02-15
- data_decisao: 2026-02-15
- autor_responsavel: Agno (execucao task governada)
- aprovadores: Owner do projeto

## Referencias

- constituicao_versao_alvo: `docs/CONSTITUICAO.md` (V1)
- secoes_afetadas: Secao 3 (Decisoes seed obrigatorias), Secao 4 (Evidencias e manifests), Secao 5 (Politica formal de emendas)
- issue_ticket_relacionado: `TASK_CEP_BUNDLE_CORE_F1_005_EMENDA_COST_MODEL_0025PCT_V1`

## Motivacao

Durante a instrumentacao de M3/W1/W2, a derivacao de custos identificou ausencia de regra normativa explicita para custo operacional (`REGRA AUSENTE`). Essa lacuna inviabiliza comparabilidade estavel entre metricas e dificulta calibracao de controladores sensiveis a turnover.

## Texto normativo atual

Nao existe regra normativa explicita para modelo de custo operacional em analises e simulacoes do bundle.

## Texto normativo proposto

Fica instituida a regra normativa de custo arbitrado:

- nome: `ARB_COST_0_025PCT_MOVED`
- taxa: `0.00025` (0,025% = 2,5 bps)
- base de calculo: `valor_movimentado_notional`
- formula: `custo_operacao = abs(notional_movimentado) * 0.00025`
- escopo: analises e simulacoes (M3/W1/W2 e futuras), ate definicao posterior
- vigencia: a partir de `2026-02-15`

Critico de governanca:

- esta regra deve ser aplicada de forma uniforme em metricas e controladores que dependam de custo transacional;
- revogacao ou alteracao so pode ocorrer por nova emenda formal, com evidencias comparativas de impacto.

## Impacto tecnico e operacional

- impacto_em_codigo: atualizacao de pipelines de metricas e controladores para aplicar `ARB_COST_0_025PCT_MOVED`.
- impacto_em_processos: comparacoes historicas devem explicitar quando custo arbitrado foi aplicado.
- impacto_em_dados: manifests e reports devem declarar `cost_model` e `cost_total` conforme a regra.
- impacto_em_riscos: reduz ambiguidade metodologica e melhora rastreabilidade de decisoes anti-deriva.

## Compatibilidade e migracao

- breaking_change: nao
- plano_migracao: manter campo `cost_total` existente e preencher por derivacao da regra em novos pacotes; reprocessar historico apenas quando requerido por task dedicada.
- rollback_plan: revogar por nova emenda com justificativa tecnica e evidencia de superioridade do modelo substituto.

## Evidencias requeridas

- report_md: `outputs/governanca/emendas/20260215_cost_model/report.md`
- manifest_json: `outputs/governanca/emendas/20260215_cost_model/manifest.json`
- evidence_dir: `outputs/governanca/emendas/20260215_cost_model/evidence/`
- hashes_sha256_relevantes:
  - `outputs/instrumentation/m3_w1_w2/20260215/turnover_costs_derivation.json` (artefato afetado)
  - `outputs/instrumentation/m3_w1_w2/20260215/metrics_m3_w1_w2_v2.json` (artefato afetado)

## Criterios de aceite

- criterio_1: regra `0.00025` sobre `notional` esta explicitamente definida.
- criterio_2: artefatos afetados por custo estao referenciados com rastreabilidade.
- criterio_3: revogacao condicionada a nova emenda com evidencias.

## Resultado da deliberacao

- decisao_final: aprovada
- justificativa_final: lacuna normativa de custo foi eliminada com regra objetiva, rastreavel e imediatamente aplicavel em metricas/controladores.
