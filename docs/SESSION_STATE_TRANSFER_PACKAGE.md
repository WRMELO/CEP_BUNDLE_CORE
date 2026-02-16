# SESSION STATE TRANSFER PACKAGE - CEP_BUNDLE_CORE

Atualizado em: `2026-02-16T00:30:19.276063+00:00`
Worktree de emissao: `/home/wilson/_wt/CEP_BUNDLE_CORE/bootstrap`
Branch: `wt/bootstrap`
HEAD: `2632ec7f838df93c25a5a397feab0929adf29c26`

## 1. CONTEXTO GERAL DO PROJETO

O `CEP_BUNDLE_CORE` consolida governanca, corpus historico e instrumentacao quantitativa para operar o bundle com foco em robustez, rastreabilidade e anti-deriva (especialmente W2). O projeto trabalha com SSOT, manifests hasheados e gates objetivos por task.

## 2. ARQUITETURA ATUAL (repos, worktrees, branches)

- Repositorio principal: `/home/wilson/CEP_BUNDLE_CORE`.
- Remoto: `git@github.com:WRMELO/CEP_BUNDLE_CORE.git`.
- Worktrees ativos:
  - `/home/wilson/CEP_BUNDLE_CORE` -> `main`
  - `/home/wilson/_wt/CEP_BUNDLE_CORE/bootstrap` -> `wt/bootstrap`
  - `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis` -> `wt/analysis-gaps`
  - `/home/wilson/_wt/CEP_BUNDLE_CORE/ssot` -> `wt/ssot`
  - `/home/wilson/_wt/CEP_BUNDLE_CORE/experiments` -> `wt/experiments`

## 3. PADRAO DE GOVERNANCA (Agno, gates, manifests, hashes)

- Toda task governada produz `report.md`, `manifest.json` e `evidence/`.
- Gates devem ser declarados gate-a-gate com criterio objetivo de PASS/FAIL.
- Manifest registra hashes SHA-256 de outputs/evidencias.
- Sync remoto (fetch/pull --ff-only/push) e evidenciado quando requerido.

## 4. PAPEIS OPERACIONAIS (Owner, CTO, Agente, Agno)

- Owner: define intencao de negocio e aprova emendas/decisoes estruturais.
- CTO: traduz objetivo em task governada, gates e criterios de aceite.
- Agente: executa tarefas tecnicas, gera artefatos, nao inventa SSOT.
- Agno: orquestra execucao com precondicoes, rastreabilidade e sync.

## 5. CONSTITUICAO VIGENTE (resumo estruturado)

Constituicao vigente: `docs/CONSTITUICAO.md` (V1), com pilares:
- runtime oficial `/home/wilson/PortfolioZero/.venv/bin/python`;
- formato canonico Parquet;
- periodos D/W/M explicitados;
- RL Variante 2 como referencia;
- Plotly como visualizacao padrao;
- governanca por gates + emendas formais.

## 6. EMENDAS ATIVAS (custos 0.025%, cash CDI, etc)

- `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`:
  - regra `ARB_COST_0_025PCT_MOVED`, taxa `0.00025` sobre `abs(notional)`.
- `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`:
  - caixa elegivel deve remunerar CDI;
  - aplicacao condicionada a SSOT CDI rastreavel.

## 7. MASTERPLAN (com referencia cruzada)

Documento: `docs/MASTERPLAN.md`.
Resumo: M3 fixado como baseline; SPC/CEP como nucleo (burners + carteira); RL subordinado ao regime; plano anti-deriva W2; fase obrigatoria de fechamento de gaps antes de evolucao estrutural.

## 8. CORPUS DE CONHECIMENTO (origem e finalidade)

Corpus consolidado em `docs/corpus/` a partir de `CEP_NA_BOLSA` e `CEP_COMPRA` para servir de base obrigatoria de desenvolvimento e auditoria historica.

Indicadores centrais do `corpus_index.json`:
- `files_scanned_count`: 1180
- `experiments_count`: 27
- `artifact_refs_count`: 7130
- `artifacts_hashed_count`: 400

## 9. INSTRUMENTACAO M3/W1/W2 (v1 -> v4)

- v1 (`metrics_m3_w1_w2.json`): metricas base M3/W1/W2 sem custo.
- v2 (`metrics_m3_w1_w2_v2.json`): adiciona turnover e `num_trades`; custo ainda `REGRA AUSENTE`.
- v3 (`metrics_m3_w1_w2_v3.json`): aplica emenda de custo 0.025% e gera metricas liquidas.
- v4 (`metrics_m3_w1_w2_v4.json`): preparado para CDI no caixa; na execucao original ficou `SSOT AUSENTE`.

## 10. ESTADO ATUAL DAS METRICAS

Base v1:
- M3_full: multiple `2.385440`, MDD `-0.680335`, equity_final `2.385440`
- W1: multiple `5.369645`, MDD `-0.187372`
- W2: multiple `0.467346`, MDD `-0.598100`

Com custo (v3):
- M3_full cost_total `0.086231`, multiple_liquido `2.299208`
- W1 cost_total `0.038922`, multiple_liquido `5.330723`
- W2 cost_total `0.025584`, multiple_liquido `0.462577`

CDI no caixa (v4 historico):
- status global: `SSOT AUSENTE`
- aplicado: `False`

## 11. GAPS AINDA ABERTOS

- Aplicar novamente accrual CDI sobre metricas agora que SSOT CDI foi criado no worktree `analysis`.
- Consolidar SSOT CDI em caminho definitivo de referencia compartilhado entre worktrees (se desejado no `bootstrap`).
- Avancar gaps G1-G4 do Masterplan (cobertura de metricas/quant/hashes/taxonomia de janelas).

## 12. SSOTs EXISTENTES E AUSENTES

Existentes:
- SSOT CDI canonizado em `analysis`: `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis/data/ssot/cdi/cdi_daily.parquet`.
- SSOTs de corpus/carteira herdados de legados conforme `docs/corpus/*` e fontes referenciadas.

Ausencias/pendencias:
- no historico da task v4, CDI estava ausente no escopo daquele momento (`SSOT AUSENTE` registrado).
- falta decidir se o SSOT CDI sera espelhado/canonizado no `bootstrap` e/ou `main`.

## 13. PADRAO DE DECISAO (M3 como planta, SPC/CEP em burners e carteira, RL subordinado)

- M3 e a planta baseline.
- SPC/CEP governa permissao/regime em burners e carteira.
- RL e camada de priorizacao sob restricoes, sem sobrepor guardrails SPC/CEP.
- Mudancas estruturais exigem emenda + evidencias comparativas.

## 14. PROXIMO PASSO TECNICO LOGICO

Executar task de reaplicacao CDI sobre pacote v3 com SSOT CDI existente (gerar v5), com:
- busca do SSOT em `/home/wilson/_wt/CEP_BUNDLE_CORE/analysis/data/ssot/cdi/cdi_daily.parquet`;
- derivacao de ganho CDI por periodo (M3_full/W1/W2);
- atualizacao de `multiple_liquido_apos_custos_e_cdi`;
- report/manifest/evidence completos e sync remoto.

## 15. REGRAS DE INTERACAO EM CHAT (protocolo Owner-CTO-Agente-Agno)

Protocolo operacional recomendado:
1. Owner envia `task_id`, `intent`, `inputs`, `outputs`, `steps`, `gates`, `acceptance_criteria`.
2. Agente responde sempre no formato: nome da task -> estado geral -> gate a gate -> caminhos dos artefatos -> comentarios.
3. CTO valida objetividade de evidencias e decide desbloqueio de prox task.
4. Agno executa sync e fecha gate de limpeza do worktree.
5. Em caso de falha, resposta obrigatoria inclui: gate falho, evidencia concreta e proxima acao objetiva.
