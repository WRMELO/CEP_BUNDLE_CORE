# Retomada — Plano vs Execução (CEP_NA_BOLSA)

Este documento relaciona o **plano geral (Constituição)** com o que já foi **implementado/validado** no repositório, indicando **onde estão os artefatos**, o que **foi feito certo** e o que **foi feito errado ou falhou** até aqui.

Base de referência do plano:  
`docs/00_constituicao/CEP_NA_BOLSA_CONSTITUICAO_V1.md`

---

## 1) Propósito do Projeto (Constituição §1)

**Plano:**  
Sistema de gestão baseado em **CEP**, sem previsão, com operação condicionada a controle estatístico e auditabilidade total.

**Status na execução:**  
- **Certo:** A estrutura de execução com rastreabilidade (Agno + outputs + audits) foi implantada.  
- **Errado/Falta:** Ainda não há implementação de cartas CEP, nem regras Western/Nelson.

**Onde está o que fizemos:**
- Execução rastreável: `scripts/agno_runner.py`
- Registros de execução: `planning/runs/*/report.json`
- Auditorias: `outputs/*/AUDIT*.md`

---

## 2) Filosofia Central (Constituição §2)

**Plano:**  
CEP como camada primária de decisão; nada fora de controle estatístico.

**Status na execução:**  
- **Certo:** Ainda não implementamos nada que viole essa regra (nenhuma decisão operacional foi criada).  
- **Falta:** Nenhuma lógica CEP implementada ainda.

**Onde está o que fizemos:**
- Estrutura pronta para rodar tasks CEP (sem lógica ainda): `planning/task_specs/*`

---

## 3) Arquitetura de Papéis (Constituição §3)

**Plano:**  
Owner decide, Planejador planeja, Orquestrador executa, Agente implementa.

**Status na execução:**  
- **Certo:** Modelo respeitado: tasks executadas via Agno, alterações via Cursor.  
- **Falta:** Nenhum mecanismo automático para garantir validações do Owner além dos reports.

**Onde está o que fizemos:**
- Tarefas e execução: `planning/task_specs/` e `planning/runs/`

---

## 4) Ferramentas Oficiais (Constituição §4)

**Plano:**  
Agno, Cursor, Obsidian, Mermaid, Miro.

**Status na execução:**  
- **Certo:** Agno e Cursor em uso; Obsidian ignorado no git.  
- **Falta:** Mermaid/Miro ainda não usados.

**Onde está o que fizemos:**
- Agno runner: `scripts/agno_runner.py`
- Exclusão Obsidian: `.gitignore` (inclui `.obsidian/`)

---

## 5) Estrutura Conceitual (Constituição §5)

**Plano:**  
Master (mercado) e Queimadores (ativos) com cartas CEP.

**Status na execução:**  
- **Falta total:** nenhuma carta CEP, nenhum cálculo de X_t, nenhuma lógica Master/Queimadores.

**Onde está o que fizemos:**
- Dados base para o futuro Master (falhou no fluxo antigo com Yahoo; ver §6)
- Dados base para universo de ativos (parcialmente prontos)

---

## 6) Dados e Variável Fundamental (Constituição §6)

**Plano:**  
Fonte de preços: yfinance. Variável: log-retorno.

**Status na execução:**  
- **Errado/Conflito:** O projeto passou a usar **brapi.dev** por limitação do Yahoo, contrariando o plano original.  
  Isso foi necessário para viabilizar continuidade, mas está **em conflito com a constituição**.
- **Falta:** Nenhum cálculo de X_t realizado.

**Onde está o que fizemos:**
- Tentativa Yahoo: `outputs/fase1/f1_002/*` (FAIL por rate-limit)
- Probe BRAPI (SDK e API): `outputs/fase1/f1_probe_sdk/*`
- Prova de BDRs via quote direto (BRAPI): `outputs/fase1/f1_bdr_direct_probe/*`

---

## 7) Fase I (Calibração 2022) (Constituição §7)

**Plano:**  
Usar 2022 para calibrar limites e estabilidade.

**Status na execução:**  
### 7.1 Master (^BVSP 2022)
- **Errado/Falhou:** Yahoo rate-limit impediu validação do Master no fluxo inicial.  
- **Falta:** Nenhum cálculo de limites ou estabilidade.

### 7.2 Universo de ativos
**Certo (parcial):**
1) **Universo NM + N2 (ações)**  
   - **Encontrado:** 210 tickers  
   - **Origem:** API B3 (listados)  
   - **Artefatos:**  
     `outputs/fase1/f1_003/universe_nm_n2_tickers.txt`  
     `outputs/fase1/f1_003/universe_nm_n2_audit.json`

2) **Universo BDR P2/P3 (32/33)**  
   - **Listagem via BRAPI (type=bdr):** **FAIL**, vazia  
   - **Heurística via universo geral + validação quote:** **PASS**  
   - **Artefatos:**  
     `outputs/fase1/f1_bdr_universe_heuristic/validated_bdr_p2_p3_tickers.txt`

3) **SSOT BDR oficial (B3 XLSX)**  
   - **PASS** com versionamento + hash  
   - **Artefatos:**  
     `outputs/ssot/bdr/b3/20260204/ssot_bdr_b3.csv`  
     `outputs/ssot/bdr/b3/20260204/manifest.json`

---

## 8) Seleção de Ativos (Constituição §8)

**Plano:**  
Somente ativos em controle estatístico, ranking secundário.

**Status na execução:**  
 - **Falta total:** Nenhuma regra de seleção implementada.

---

## 9) Qualidade e Disciplina (Constituição §9)

**Plano:**  
Nenhuma decisão sem log, nenhuma regra implícita, auditabilidade total.

**Status na execução:**  
 - **Certo:** Todas as tasks executadas via Agno têm report e evidências.  
 - **Falta:** Não há ainda um “log operacional” contínuo.

**Onde está o que fizemos:**
- Logs e reports: `planning/runs/*`
- Auditorias: `outputs/*/AUDIT*.md`

---

## 10) Instrução ao Próximo Chat (Constituição §10)

**Plano:**  
Planejador deve iniciar pela Fase I e usar Agno.

**Status na execução:**  
 - **Certo:** Workflows estruturados com Agno, execução de tasks com rastreabilidade.

---

## Inventário de Tasks e Resultado Real

| Task | Objetivo | Status | Evidência |
|---|---|---|---|
| `TASK_CEP_F1_001` | Bootstrap Agno runtime | **PASS** | `planning/runs/TASK_CEP_F1_001/report.json` |
| `TASK_CEP_F1_002` (SSOT BDR B3) | SSOT oficial B3 (XLSX) | **PASS** | `planning/runs/TASK_CEP_F1_002/report.json` |
| `TASK_CEP_DISC_F1_003` | BDR P2/P3 via `quote/list?type=bdr` | **FAIL** | `planning/runs/TASK_CEP_DISC_F1_003/report.json` |
| `TASK_CEP_DISC_F1_PROBE_SDK_001` | Probe SDK brapi quote/list | **PASS** | `planning/runs/TASK_CEP_DISC_F1_PROBE_SDK_001/report.json` |
| `TASK_CEP_DISC_F1_BDR_DIRECT_PROBE_001` | Provar BDR via quote direto | **PASS** | `planning/runs/TASK_CEP_DISC_F1_BDR_DIRECT_PROBE_001/report.json` |
| `TASK_CEP_DISC_F1_BDR_P2P3_UNIVERSE_HEURISTIC_001` | Universo P2/P3 via heurística | **PASS** | `planning/runs/TASK_CEP_DISC_F1_BDR_P2P3_UNIVERSE_HEURISTIC_001/report.json` |

**Nota:** O universo **NM + N2 (210 tickers)** foi gerado por script manual, sem task Agno:
- Script: `scripts/fetch_universe_nm_n2_tickers.py`
- Outputs:  
  `outputs/fase1/f1_003/universe_nm_n2_tickers.txt`  
  `outputs/fase1/f1_003/universe_nm_n2_audit.json`

---

## O que foi feito certo (resumo)

- Runtime Agno instalado e funcionando.  
- SSOT oficial de BDRs via XLSX da B3 com hash e manifesto.  
- Provas de BDRs acessíveis via cotação direta (BRAPI).  
- Heurística funcional para universo P2/P3 (32/33) baseada em universo geral e validação por quote.  
- Auditorias e evidências arquivadas corretamente.

---

## O que foi feito errado / falhou / conflitante

- **Falha real** no endpoint `quote/list?type=bdr` (BRAPI): vazio.  
  Evidência: `outputs/fase1/f1_003/diagnostic_matrix.json`
- **Conflito com a Constituição**: uso de BRAPI como fonte de dados, enquanto o plano original define yfinance.  
  (Foi necessário por rate-limit do Yahoo, mas requer revisão formal da constituição.)
- **Master 2022 (Yahoo)**: falhou por rate-limit; não há master validado nem limites CEP.

---

## Próximos passos (para o planejador alinhar plano vs realidade)

1) **Decidir formalmente** se BRAPI será fonte oficial (atualizar constituição) ou se voltamos ao Yahoo com outra estratégia.  
2) **Criar task Agno** para o universo NM+N2 (se quiser rastreabilidade formal).  
3) **Implementar Fase I real**: cálculo de X_t, cartas CEP, Master/Queimadores.  
4) **Integrar SSOT BDR B3** ao pipeline final de universo.

---

## Onde está o plano geral

`docs/00_constituicao/CEP_NA_BOLSA_CONSTITUICAO_V1.md`

