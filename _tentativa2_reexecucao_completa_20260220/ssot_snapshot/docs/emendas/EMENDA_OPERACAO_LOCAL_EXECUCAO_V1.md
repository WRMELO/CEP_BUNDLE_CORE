# EMENDA CONSTITUCIONAL - OPERACAO LOCAL DE EXECUCAO (V1)

## Metadados

- emenda_id: E-2026-02-16-OPERACAO-LOCAL-EXECUCAO-V1
- versao_emenda: V1
- status: aprovada
- data_proposta: 2026-02-16
- data_decisao: 2026-02-16
- autor_responsavel: Agno (execucao task governada)
- aprovadores: Owner do projeto

## Referencias

- constituicao_versao_alvo: `docs/CONSTITUICAO.md` (V1)
- secoes_afetadas: Secao 3.7 (Git e worktrees), Secao 4 (Evidencias e manifests), Secao 5 (Politica formal de emendas)
- issue_ticket_relacionado: `TASK_CEP_BUNDLE_CORE_F0_015_RESTORE_LOCAL_EXECUTION_STRUCTURE_AND_NORMATIVE_V1`

## Motivacao

A auditoria de Masterplan e conformidade identificou `NO_EVIDENCE` para normalizacao permanente do modo de operacao em `LOCAL_ROOT`. Esta emenda formaliza o padrao local e elimina ambiguidade operacional.

## Texto normativo proposto

Fica instituido o modo operacional padrao do bundle:

- modo_padrao: `LOCAL_ROOT_ONLY`
- repo_root_oficial: `/home/wilson/CEP_BUNDLE_CORE`
- estrutura obrigatoria de execucao:
  - `planning/task_specs/`
  - `planning/runs/`
  - `outputs/governanca/<pacote>/...`

Regras complementares:

1. Execucao normal de tasks nao deve depender de caminhos em `/home/wilson/_wt/*`.
2. Worktrees podem existir como apoio historico/isolamento, mas nao como dependencia obrigatoria da operacao padrao.
3. Cada task deve manter trilha de evidencias em `report.md`, `manifest.json` e `evidence/`.
4. Qualquer mudanca desse modo padrao exige nova emenda formal com comparativo de impacto.

## Compatibilidade com a Constituicao

Esta emenda complementa `docs/CONSTITUICAO.md` sem contradizer seus principios:

- preserva reprodutibilidade e evidencias (Secao 2 e 4);
- preserva governanca incremental por emendas (Secao 5);
- especifica como aplicar a politica Git/worktrees em contexto de operacao local consolidada (Secao 3.7).

## Impacto tecnico e operacional

- padroniza local unico de execucao para reduzir deriva operacional;
- restaura estrutura `planning/*` no repo local;
- melhora auditabilidade do fluxo Masterplan -> subtasks -> runs.

## Criterios de aceite

- `planning/task_specs/` e `planning/runs/` presentes no repo local;
- `docs/EXECUTION_STRUCTURE_LOCAL.md` publicado;
- smoke-run local comprovando escrita em `planning/runs/` e `outputs/governanca/`;
- pacote de governanca da task com hashes e evidencias.

## Resultado da deliberacao

- decisao_final: aprovada
- justificativa_final: normalizacao do modo local elimina nao conformidade de evidencia e preserva aderencia a Constituicao por via formal de emenda.
