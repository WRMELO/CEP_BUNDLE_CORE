# EMENDA CONSTITUCIONAL - CASH REMUNERADO A CDI (V1)

## Metadados

- emenda_id: E-2026-02-15-CASH-CDI-V1
- versao_emenda: V1
- status: aprovada
- data_proposta: 2026-02-15
- data_decisao: 2026-02-15
- autor_responsavel: Agno (execucao task governada)
- aprovadores: Owner do projeto

## Referencias

- constituicao_versao_alvo: `docs/CONSTITUICAO.md` (V1)
- secoes_afetadas: Secao 3 (Decisoes seed obrigatorias), Secao 5 (Politica formal de emendas)
- issue_ticket_relacionado: `TASK_CEP_BUNDLE_CORE_F1_008_VERIFY_CDI_CASH_REMUNERATION_DOCS_AND_EMEND_IF_MISSING_V1`

## Motivacao

A verificacao objetiva em `docs/CONSTITUICAO.md` e `docs/emendas/` nao encontrou regra normativa explicita de remuneracao do caixa por CDI. A ausencia gera ambiguidade em simulacoes e comparacoes de performance quando houver alocacao parcial em caixa.

## Texto normativo atual

Nao existe regra explicita sobre remuneracao de caixa por CDI na Constituicao V1 ou emendas vigentes.

## Texto normativo proposto

Fica instituida a regra normativa:

- nome: `CASH_REMUNERACAO_CDI_V1`
- definicao: todo saldo de caixa elegivel deve ser remunerado pela taxa CDI no periodo correspondente.
- escopo: analises, simulacoes e controladores do bundle, incluindo janelas historicas (W1/W2/W3 e futuras) quando houver componente de caixa.
- aplicacao operacional: taxa CDI deve ser convertida para a granularidade da simulacao (ex.: diaria), com formula explicitada em report/manifest do experimento.
- rastreabilidade: artefatos devem declarar fonte da serie CDI, regra de conversao e impacto na curva de equity.

Condicao de alteracao:

- qualquer alteracao, suspensao ou excecao desta regra so pode ocorrer por nova emenda formal com evidencias comparativas.

## Impacto tecnico e operacional

- impacto_em_codigo: pipelines de backtest/metricas devem incluir remuneracao do caixa por CDI.
- impacto_em_processos: reports devem explicitar se CDI foi aplicado e com qual serie/frequencia.
- impacto_em_dados: necessidade de serie CDI versionada e rastreavel.
- impacto_em_riscos: reduz risco de subestimacao/superestimacao de performance por tratamento inconsistente de caixa.

## Compatibilidade e migracao

- breaking_change: nao
- plano_migracao: aplicacao obrigatoria em novos pacotes; reprocessamento historico quando demandado por task dedicada.
- rollback_plan: somente por emenda superveniente com justificativa e evidencias.

## Evidencias requeridas

- report_md: `outputs/governanca/verify_cdi_cash/20260215/report.md`
- manifest_json: `outputs/governanca/verify_cdi_cash/20260215/manifest.json`
- evidence_dir: `outputs/governanca/verify_cdi_cash/20260215/evidence/`
- hashes_sha256_relevantes:
  - `outputs/governanca/verify_cdi_cash/20260215/evidence/search_results.txt`
  - `docs/MASTERPLAN.md` (referencia cruzada adicionada)

## Criterios de aceite

- criterio_1: regra de caixa remunerado a CDI definida de forma clara e verificavel.
- criterio_2: condicoes de alteracao restringidas a nova emenda formal.
- criterio_3: referencia cruzada adicionada no `docs/MASTERPLAN.md`.

## Resultado da deliberacao

- decisao_final: aprovada
- justificativa_final: lacuna normativa foi fechada com regra operacional clara para tratamento de caixa em analises e controladores.
