# EXECUTION STRUCTURE LOCAL

- atualizado_em_utc: `2026-02-16T11:12:41.080480+00:00`
- modo_padrao: `LOCAL_ROOT_ONLY`
- repo_root: `/home/wilson/CEP_BUNDLE_CORE`

## Estrutura obrigatoria

- `planning/task_specs/`: especificacoes de tasks para execucao local.
- `planning/runs/`: registros de execucao por task e run.
- `outputs/governanca/<pacote>/...`: relatorios, manifest e evidencias.

## Como executar em modo LOCAL_ROOT

1. Criar/atualizar spec em `planning/task_specs/`.
2. Executar runner local com `repo_root=/home/wilson/CEP_BUNDLE_CORE`.
3. Persistir output tecnico em `outputs/governanca/...`.
4. Registrar run em `planning/runs/<TASK_ID>/run_<timestamp>/`.

## Evidencias minimas obrigatorias por task

- `report.md` autocontido
- `manifest.json` com hashes SHA-256
- `evidence/` com logs/comandos e status git

## Regras operacionais

- Nao depender de `/home/wilson/_wt/*` para execucao normal.
- Manter rastreabilidade de branch/HEAD em cada pacote de governanca.
- Respeitar Constituicao e Emendas vigentes em `docs/CONSTITUICAO.md` e `docs/emendas/`.
