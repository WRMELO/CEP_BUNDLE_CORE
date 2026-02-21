# Runner Xbarra–R (Plotly) — Template de uso

Este runner gera Xbarra–R interativo (Plotly) com subgrupos rolling-overlapping (stride=1) e manifest.json com hashes.

## Pré-requisitos

- `plotly` instalado no `.venv` (PortfolioZero).
- Política do projeto: o runner **não** instala dependências automaticamente.

## Execução

```bash
python -m cep.runners.runner_xbarr_r_plotly_v1 --config docs/templates/runner_xbarr_r_plotly_v1.config.json
```

## Observações

- Este runner assume **rolling_overlapping** como modo padrão (experimental).  
- O `window_n` deve ser igual ao `N_master` do SSOT em `docs/ssot/master_calibration.json`.
- Se a base operacional tiver outros nomes de coluna, ajuste `column_mapping`.
