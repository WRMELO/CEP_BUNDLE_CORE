# LOCAL DOCUMENT INDEX - CEP_BUNDLE_CORE

- atualizado_em_utc: `2026-02-16T09:55:29.424164+00:00`
- local_root_rel: `.`
- local_root_abs: `/home/wilson/CEP_BUNDLE_CORE`
- branch_integracao: `local/integrated-state-20260215`
- head_integracao: `0e5a122cfebe26feabc5d22f665e3645225e35fd`

## Documentos-chave (entrada humana)

| Documento | Path relativo | Path absoluto | Descricao |
|---|---|---|---|
| Constituicao vigente | `docs/CONSTITUICAO.md` | `/home/wilson/CEP_BUNDLE_CORE/docs/CONSTITUICAO.md` | Regra normativa principal do bundle e governanca. |
| Masterplan | `docs/MASTERPLAN.md` | `/home/wilson/CEP_BUNDLE_CORE/docs/MASTERPLAN.md` | Plano estrategico com baseline M3, SPC/CEP e gaps G1-G4. |
| Emenda de custos | `docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md` | `/home/wilson/CEP_BUNDLE_CORE/docs/emendas/EMENDA_COST_MODEL_ARB_0025PCT_V1.md` | Regra formal de custo 0.025% por notional movimentado. |
| Emenda CDI caixa | `docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md` | `/home/wilson/CEP_BUNDLE_CORE/docs/emendas/EMENDA_CASH_REMUNERACAO_CDI_V1.md` | Regra de remuneracao de caixa por CDI. |
| Pacote de transferencia de sessao (MD) | `docs/SESSION_STATE_TRANSFER_PACKAGE.md` | `/home/wilson/CEP_BUNDLE_CORE/docs/SESSION_STATE_TRANSFER_PACKAGE.md` | Resumo completo para continuidade em novo chat. |
| Pacote de transferencia de sessao (JSON) | `docs/SESSION_STATE_TRANSFER_PACKAGE.json` | `/home/wilson/CEP_BUNDLE_CORE/docs/SESSION_STATE_TRANSFER_PACKAGE.json` | Estrutura machine-readable do estado operacional. |
| SSOT CDI README | `data/ssot/cdi/README.md` | `/home/wilson/CEP_BUNDLE_CORE/data/ssot/cdi/README.md` | Proveniencia, schema e cobertura da serie CDI diaria. |
| SSOT CDI Parquet | `data/ssot/cdi/cdi_daily.parquet` | `/home/wilson/CEP_BUNDLE_CORE/data/ssot/cdi/cdi_daily.parquet` | Serie CDI diaria canonizada em Parquet. |
| Governanca session transfer report | `outputs/governanca/session_state_transfer/20260215/report.md` | `/home/wilson/CEP_BUNDLE_CORE/outputs/governanca/session_state_transfer/20260215/report.md` | Evidencia da consolidacao de estado de sessao. |
| Governanca SSOT CDI report | `outputs/governanca/ssot_cdi/20260215/report.md` | `/home/wilson/CEP_BUNDLE_CORE/outputs/governanca/ssot_cdi/20260215/report.md` | Evidencia de criacao/canonizacao do SSOT CDI. |
| Governanca local migration report | `outputs/governanca/local_migration/20260216/report.md` | `/home/wilson/CEP_BUNDLE_CORE/outputs/governanca/local_migration/20260216/report.md` | Evidencia desta migracao para operacao local unica. |

## Rastreabilidade de linhas consolidadas (contexto historico)

- linha bootstrap (contexto): `bcc58322691f94a9057ced8df3ed44592caca2c5`
- linha analysis (contexto): `ac25c3b0f1dcf66306dba44780dd51bc99bc0d75`
- estado integrado local atual: branch `local/integrated-state-20260215` no HEAD acima.

## Modo operacional padrao (LOCAL_ROOT)

- Proximas tasks devem usar `repo_root=/home/wilson/CEP_BUNDLE_CORE`.
- Nao depender de paths em `/home/wilson/_wt/*` para leitura de artefatos minimos.
- Worktrees permanecem apenas como contexto historico/backup nesta fase.
