# Recommend Next Subtasks - Return to Masterplan

## Prioridade imediata (retomar filosofia Masterplan -> subtasks -> retorno)

1. `TASK_CEP_BUNDLE_CORE_F1_012_APPLY_CDI_SSOT_TO_METRICS_V5`
   - Objetivo: aplicar SSOT CDI existente ao pacote v3 e fechar `cash_cdi_status=APPLIED` quando suportado por evidencia.
   - Entradas: `data/ssot/cdi/cdi_daily.parquet`, `outputs/instrumentation/m3_w1_w2/20260215_costs_v3/metrics_m3_w1_w2_v3.json`.
   - Saida esperada: metrics v5 + derivacao + report/manifest/evidence.

2. `TASK_CEP_BUNDLE_CORE_F1_015_MASTERPLAN_E1_E2_ABLATION_START`
   - Objetivo: abrir e executar E1 e E2 do plano de experimentos com comparativos objetivos versus baseline M3.
   - Saida esperada: pacote comparativo com gates e hashes.

3. `TASK_CEP_BUNDLE_CORE_F1_016_MASTERPLAN_E3_E5_ABLATION_CLOSE`
   - Objetivo: completar E3..E5 e fechar criterio de aceite do bloco de experimentos.

4. `TASK_CEP_BUNDLE_CORE_F1_017_CLOSE_GAPS_G1_G4_WITH_THRESHOLDS`
   - Objetivo: atacar gaps G1..G4 do Masterplan com metas quantitativas explicitas e evidencia de cumprimento.

## Regra de execucao

A cada subtask concluida: atualizar mapeamento Masterplan->Tasks, reavaliar gaps/orfaos/compliance, e somente entao avancar para o proximo item do Masterplan.
