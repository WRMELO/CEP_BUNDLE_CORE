# Checklist — Estado Confirmado (Owner) — CEP_BUNDLE_CORE (Tentativa 2)

last_updated: 2026-02-23
scope: Tentativa 2 (work/ e outputs/ apenas; ssot_snapshot/ read-only)

Regra: este checklist mostra apenas o que existe como correto (confirmado e materializado). Nao listar pendencias.
Regra permanente: CTO sempre entrega O QUE/POR QUE/COMO/RESULTADO ESPERADO em linguagem natural (sem código) antes do JSON ao Agente; regras só valem se usadas desde S001 com evidência; senão vai para Crítica do Owner.

```mermaid
flowchart TB
  S001["S001 Universo promovido confirmado: 1690 tickers (inclui ^BVSP)"]
  S002["S002 Séries de referência canônicas: S&P500 diário + CDI diário"]
  S003["S003 SSOTs referência atualizados 2018..2026 (Ações/BDRs/BVSP/CDI/SP500)"]
  S004["S004 Guideline SPC/CEP (Burners + Master) registrado"]
  S005["S005 SSOT tickers aprovados (448)"]
  S006["S006 Base Operacional Canônica Completa (448)"]
  S007["S007 Ranking Burners diário OEE LP/CP (448) — V2 warmup buffer"]
  S008["S008 Candidatos: Campeonato F1 + slope_45 (filtro slope_60>0)"]
  S001 --> S002 --> S003 --> S004 --> S005 --> S006 --> S007 --> S008
```

## Atualizacoes ativas de fonte (governanca)

- update_date: 2026-02-23
- S007 ACTIVE (RULEFLAGS 20260223): `/home/wilson/CEP_BUNDLE_CORE/_tentativa2_reexecucao_completa_20260220/outputs/state/s007_ruleflags/20260223/s007_ruleflags.parquet`
- S007 INACTIVE/DEPRECATED (nao deletar): `/home/wilson/CEP_BUNDLE_CORE/_tentativa2_reexecucao_completa_20260220/outputs/burners_ranking_oee_lp_cp_daily_v2_warmup_buffer/ranking/burners_ranking_daily.parquet`
- justificativa: habilita auditoria por regra Nelson/W.E. com flags materializadas por regra e agregados determinísticos.
