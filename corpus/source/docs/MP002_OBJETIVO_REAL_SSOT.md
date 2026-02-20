# MP002 OBJETIVO REAL SSOT

- origem: `docs/MASTERPLAN.md` (secao `## Objetivo real (anti-deriva e anti-bloqueio)`)
- gerado_em_utc: `2026-02-16T12:28:21.752527+00:00`
- masterplan_item: `MP-002`

## Funcao objetivo operacional

Maximizar robustez de resultado em janelas adversas (W2-like) sem bloquear indevidamente a capacidade de operacao em regimes favoraveis.

## Prioridades

1. Reduzir deriva destrutiva em W2 (drawdown/turnover sob controle).
2. Evitar bloqueio operacional por excesso de restricao.
3. Preservar hierarquia de decisao: SPC/CEP governa regime; RL atua apenas na priorizacao intra-regime.

## Guardrails ativos (ancorados em pacote anti-deriva)

- experimento incremental selecionado para W2: `E4_ADD_ANTI_REENTRY_FILTER`;
- ganho W2 (delta_multiple): `0.2328437866971526`;
- melhoria W2 (delta_MDD): `0.2294853074704355`;
- histerese de regime: on `-0.2` / off `-0.1`;
- teto de turnover por regime conforme `outputs/controle/anti_deriva_w2/20260216/anti_deriva_w2_summary.json`.

## Metricas de sucesso

- robustez em W2 superior ao baseline em `multiple` e `MDD`;
- cumprimento dos guardrails de rotatividade e fallback em estresse;
- manutencao de rastreabilidade: `report.md` + `manifest.json` + `evidence/`.

## Fontes SSOT para este item

- `docs/MASTERPLAN.md`
- `outputs/controle/anti_deriva_w2/20260216/anti_deriva_w2_summary.json`
- `outputs/governanca/policy_spc_rl/20260216/policy_spc_rl_summary.json`
- `docs/CONSTITUICAO.md`
