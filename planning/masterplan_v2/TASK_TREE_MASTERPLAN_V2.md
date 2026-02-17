# TASK TREE - MASTERPLAN V2

- modo: `generate_only`
- escopo: planejamento completo até conclusão do V2, sem execução
- branch_requerida: `local/integrated-state-20260215`

## Fase 0

### TASK_CEP_BUNDLE_CORE_V2_F0_001_SSOT_MASTERPLAN_V2_PUBLICATION

- **Objetivo:** Publicar MASTERPLAN_V2 como SSOT documental com rastreabilidade e hashes.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `docs/MASTERPLAN_V2.md`
  - `outputs/governanca/masterplan_v2/f0_001/report.md`
  - `outputs/governanca/masterplan_v2/f0_001/manifest.json`
- **Dependências:**
  - nenhuma
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F0_002_GOVERNANCE_INDEX_AND_BOOTSTRAP

- **Objetivo:** Atualizar índice local e registrar pacote inicial de governança do V2.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `docs/LOCAL_DOCUMENT_INDEX.md`
  - `outputs/governanca/masterplan_v2/f0_002/report.md`
  - `outputs/governanca/masterplan_v2/f0_002/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F0_001_SSOT_MASTERPLAN_V2_PUBLICATION`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

## Fase 1

### TASK_CEP_BUNDLE_CORE_V2_F1_001_LEDGER_DAILY_PORTFOLIO_INTEGRITY

- **Objetivo:** Validar integridade contábil de ledger/daily_portfolio com custos e CDI.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f1_001/validation_report.md`
  - `outputs/masterplan_v2/f1_001/manifest.json`
  - `outputs/masterplan_v2/f1_001/evidence/`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F0_002_GOVERNANCE_INDEX_AND_BOOTSTRAP`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F1_002_ACCOUNTING_PLOTLY_DECOMPOSITION

- **Objetivo:** Gerar decomposição Plotly de P&L/custos/caixa para baseline e NEW.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f1_002/pnl_decomposition.html`
  - `outputs/masterplan_v2/f1_002/report.md`
  - `outputs/masterplan_v2/f1_002/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F1_001_LEDGER_DAILY_PORTFOLIO_INTEGRITY`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
  - plot(s) Plotly em HTML
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

## Fase 2

### TASK_CEP_BUNDLE_CORE_V2_F2_001_ENVELOPE_CONTINUO_IMPLEMENTATION

- **Objetivo:** Implementar envelope contínuo SPC/CEP com guardrails E2/E3/E4.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f2_001/envelope_daily.csv`
  - `outputs/masterplan_v2/f2_001/report.md`
  - `outputs/masterplan_v2/f2_001/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F1_002_ACCOUNTING_PLOTLY_DECOMPOSITION`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F2_002_EXECUTOR_CASH_AND_CADENCE_ENFORCEMENT

- **Objetivo:** Aplicar executor com compra só com caixa, cadência de 3 sessões e T+0 operacional.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f2_002/execution_audit.csv`
  - `outputs/masterplan_v2/f2_002/report.md`
  - `outputs/masterplan_v2/f2_002/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F2_001_ENVELOPE_CONTINUO_IMPLEMENTATION`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F2_003_ENVELOPE_PLOTLY_AUDIT

- **Objetivo:** Publicar auditoria Plotly do envelope/guardrails/fallback.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f2_003/envelope_audit.html`
  - `outputs/masterplan_v2/f2_003/report.md`
  - `outputs/masterplan_v2/f2_003/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F2_002_EXECUTOR_CASH_AND_CADENCE_ENFORCEMENT`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
  - plot(s) Plotly em HTML
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F2_004_EQUITY_CDI_SANITY_AND_RECONCILIATION_GATE

- **Objetivo:** Executar gate de sanidade numérica de CDI/equity/cashflow e bloquear avanço para F3/F4 em caso de falha.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f2_004/report.md`
  - `outputs/masterplan_v2/f2_004/manifest.json`
  - `outputs/masterplan_v2/f2_004/evidence/`
  - `outputs/masterplan_v2/f2_004/plots/`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F2_003_ENVELOPE_PLOTLY_AUDIT`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` habilitado neste gate (bloqueante para F3/F4)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence/plots)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs e inputs críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
  - `plots_plotly_html`
- **Critérios de FAIL:**
  - CDI diário fora de ordem de grandeza plausível
  - benchmark cash-only em patamar incompatível com CDI diário
  - reconciliação cashflow/equity acima da tolerância
  - normalização de equity inconsistente (primeiro ponto != 1 dentro de eps)
  - ausência de qualquer output obrigatório

## Fase 3

### TASK_CEP_BUNDLE_CORE_V2_F3_001_REVALIDATE_ABLATION_E1_E5

- **Objetivo:** Revalidar ablation E1..E5 no V2 com mesma contabilidade do baseline.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f3_001/results_summary.json`
  - `outputs/masterplan_v2/f3_001/report.md`
  - `outputs/masterplan_v2/f3_001/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F2_004_EQUITY_CDI_SANITY_AND_RECONCILIATION_GATE`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F3_002_NON_REGRESSION_WINDOW_CHECK

- **Objetivo:** Comparar full/W1/W2/W3 e validar não-regressão por métricas alvo.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f3_002/metrics_compare.csv`
  - `outputs/masterplan_v2/f3_002/report.md`
  - `outputs/masterplan_v2/f3_002/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F3_001_REVALIDATE_ABLATION_E1_E5`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

## Fase 4

### TASK_CEP_BUNDLE_CORE_V2_F4_001_RL_INTRA_REGIME_INTERFACE

- **Objetivo:** Ativar interface RL intra-regime com variáveis contínuas e trilha de inputs/ações.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f4_001/rl_action_log.parquet`
  - `outputs/masterplan_v2/f4_001/report.md`
  - `outputs/masterplan_v2/f4_001/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F3_002_NON_REGRESSION_WINDOW_CHECK`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F4_002_W2_REJECTION_RULE_VALIDATION

- **Objetivo:** Validar regra de rejeição automática para ganhos RL com piora em W2.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f4_002/rejection_decisions.json`
  - `outputs/masterplan_v2/f4_002/report.md`
  - `outputs/masterplan_v2/f4_002/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F4_001_RL_INTRA_REGIME_INTERFACE`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

## Fase 5

### TASK_CEP_BUNDLE_CORE_V2_F5_001_G1_G4_COVERAGE_CLOSEOUT

- **Objetivo:** Fechar cobertura G1..G4 do corpus e consolidar taxonomia de janelas/regimes.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/masterplan_v2/f5_001/gaps_closure.json`
  - `outputs/masterplan_v2/f5_001/report.md`
  - `outputs/masterplan_v2/f5_001/manifest.json`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F4_002_W2_REJECTION_RULE_VALIDATION`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

### TASK_CEP_BUNDLE_CORE_V2_F5_002_FINAL_GOVERNANCE_PACKAGE

- **Objetivo:** Emitir pacote final de governança V2 com recomendação de continuidade.
- **Entradas (SSOT):**
  - `docs/MASTERPLAN_V2.md`
  - `docs/CONSTITUICAO.md`
  - `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md`
  - `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md`
  - `docs/emendas/EMENDA_OPERACAO_LOCAL_EXECUCAO_V1.md`
- **Saídas:**
  - `outputs/governanca/masterplan_v2/final/report.md`
  - `outputs/governanca/masterplan_v2/final/manifest.json`
  - `outputs/governanca/masterplan_v2/final/evidence/`
- **Dependências:**
  - `TASK_CEP_BUNDLE_CORE_V2_F5_001_G1_G4_COVERAGE_CLOSEOUT`
- **Gates de aceite:**
  - `S1_GATE_ALLOWLIST`
  - `S2_CHECK_COMPILE` (quando houver código)
  - `S3_RUN` desabilitado neste ciclo (`generate_only`)
  - `S4_VERIFY_OUTPUTS` (report/manifest/evidence)
  - `S5_VERIFY_HASHES` (sha256 de SSOTs lidos e artefatos críticos)
- **Evidência mínima:**
  - `report.md`
  - `manifest.json`
  - `evidence/`
- **Critérios de FAIL:**
  - ausência de qualquer output obrigatório
  - hash ausente para SSOT consumido
  - dependência não satisfeita
  - tentativa de alterar SSOT existente sem task dedicada de mudança normativa

## Regras de planejamento do ciclo

- Não executar tasks neste ciclo; apenas gerar documentação e specs.
- Não inventar detalhes não definidos: quando houver lacuna, referenciar SSOT vigente e task materializadora.
- Manter linguagem objetiva, auditável e sem ambiguidades operacionais.
