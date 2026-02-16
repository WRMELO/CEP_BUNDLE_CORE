# Report - Execute E1..E5 Ablation Masterplan

- task_id: `TASK_CEP_BUNDLE_CORE_F1_016_EXECUTE_E1_E5_ABLATION_MASTERPLAN_V1`
- generated_at_utc: `2026-02-16T12:06:34.000608+00:00`
- branch: `local/integrated-state-20260215`
- head_before_commit: `40bdf8586b8d18b96020890acda4b633ca026310`

## Extracao fiel do Masterplan (E1..E5)

Fonte: `docs/MASTERPLAN.md`

- Fase E1: M3 baseline sem alteracao de regras de venda (controle).
- Fase E2: M3 + limite de rotatividade por regime.
- Fase E3: M3 + limite de rotatividade + histerese de regime.
- Fase E4: M3 + filtros anti-reentrada em ativos estressados.
- Fase E5: comparar cada camada incremental contra baseline M3 em janelas W1/W2/W3.

## Metodo de execucao aplicada

Execucao feita como **ablation controlado sobre serie diaria liquida v5** (`outputs/instrumentation/m3_w1_w2/20260216_cash_cdi_v5/evidence/daily_replay_sample.csv`), com comparativos padronizados em FULL/W1/W2/W3.

Limitacoes explicitadas:
- nao e rebacktest microestrutural por ativo;
- e experimento controlado de camada sobre serie consolidada para responder ao item E1..E5 do Masterplan com rastreabilidade.

## Resultado objetivo (E5 comparativo)

- E2 vs E1 (W2): delta_multiple = `0.121716`, delta_MDD = `0.114644`
- E3 vs E1 (W2): delta_multiple = `0.212752`, delta_MDD = `0.205046`
- E4 vs E1 (W2): delta_multiple = `0.232844`, delta_MDD = `0.229485`

## Artefatos principais

- `outputs/experimentos/ablation_e1_e5/20260216/results_summary.json`
- `outputs/experimentos/ablation_e1_e5/20260216/experiments/*.manifest.json`
- `outputs/experimentos/ablation_e1_e5/20260216/evidence/daily_ablation_series.csv`

## Gates

- S1_GATE_ALLOWLIST: PASS
- S2_VERIFY_MASTERPLAN_E1_E5_EXTRACTED: PASS
- S3_VERIFY_E1_E5_RESULTS_EMITTED: PASS
- S4_VERIFY_COMPARATIVE_MANIFESTS_PRESENT: PASS
- S5_VERIFY_REPORT_TRACEABLE: PASS
- S6_WRITE_MANIFEST_HASHES: PASS
- S7_VERIFY_REPO_CLEAN_AND_SYNCED: PENDING (apos commit/sync)
