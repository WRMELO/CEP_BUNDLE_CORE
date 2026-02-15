# CONSTITUICAO DO CEP_BUNDLE_CORE (V1)

## 1) Identidade e escopo

Esta Constituicao define regras obrigatorias de engenharia, governanca e rastreabilidade do `CEP_BUNDLE_CORE`.
Ela e a fonte normativa principal para bootstrap, evolucao e operacao do repositorio.

## 2) Principios normativos

1. Reprodutibilidade: fluxos devem ser deterministas e auditaveis.
2. Evidencia: toda automacao relevante deve produzir evidencias autocontidas.
3. Rastreabilidade: decisoes, artefatos e execucoes devem ser verificaveis por hash.
4. Seguranca operacional: nao expor segredos em logs, reports ou manifests.
5. Governanca incremental: mudancas estruturais via emendas formais.

## 3) Decisoes seed obrigatorias

### 3.1 Runtime oficial (venv)

- Interpretador oficial: `/home/wilson/PortfolioZero/.venv/bin/python`.
- Toda execucao de automacao deve ser compativel com esse runtime.

### 3.2 Formato de dados canonico

- Formato padrao para dados tabulares persistidos: **Parquet**.
- O uso de outros formatos exige justificativa em emenda ou report tecnico.

### 3.3 Politica de periodos

- Periodos analiticos devem ser explicitos em metadados e artefatos.
- Convencao inicial:
  - `D` (diario)
  - `W` (semanal)
  - `M` (mensal)
- Toda agregacao deve declarar periodo de origem e periodo alvo.

### 3.4 RL - Variante 2

- A estrategia de referencia de aprendizado por reforco neste bundle e a **Variante 2**.
- Mudanca de variante exige emenda formal e plano de migracao.

### 3.5 Agno e gates

- Execucoes de governanca devem seguir fluxo com gates verificaveis.
- Um gate falho impede classificacao `OVERALL PASS`.
- Reports devem declarar status gate a gate com criterio objetivo.

### 3.6 Visualizacao padrao

- Biblioteca padrao de plots interativos: **Plotly**.
- Substituicoes exigem justificativa tecnica e impacto documentado.

### 3.7 Git e worktrees

- Branch principal de integracao: `main`.
- Worktrees dedicados por fase sao obrigatorios quando definidos pela governanca.
- Mudancas de fase devem ocorrer no worktree correspondente, mantendo limpeza do root principal.

## 4) Evidencias e manifests

- Todo fluxo de governanca deve gerar:
  - `report.md` autocontido
  - `manifest.json` com metadados e hashes SHA-256
  - diretório `evidence/` com saidas dos comandos relevantes
- Criticos minimos:
  - HEAD/branch
  - status do worktree
  - comandos e mensagens de erro/sucesso

## 5) Politica formal de emendas

- Toda alteracao desta Constituicao deve ser feita por emenda em `docs/emendas/`.
- Emendas devem usar o template oficial `docs/emendas/EMENDA_TEMPLATE.md`.
- Emenda aprovada deve referenciar:
  - motivacao
  - texto normativo alterado
  - impacto tecnico e operacional
  - plano de transicao, quando aplicavel

## 6) Vigencia

Esta versao entra em vigor imediatamente como **Constituicao V1** do `CEP_BUNDLE_CORE`.

