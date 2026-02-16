# SSOT CDI Daily

- arquivo: `data/ssot/cdi/cdi_daily.parquet`
- gerado_em_utc: `2026-02-16T00:24:12.147966+00:00`
- fonte reutilizada: `/home/wilson/CEP_COMPRA/outputs/backtests/task_012/run_20260212_114129/consolidated/series_alinhadas_plot.parquet`
- sha256_fonte: `0f4bf81c90cb0d7e4bf9ded96ef3d6cf3e558ed07f8f123deb9277ed4175db7d`
- sha256_ssot: `bee97e79e0654ed79f8846558ffab359087080bd84e70d502dd219fee4eab5da`

## Schema

- `date` (datetime64[ns])
- `cdi_index_norm` (float)
- `cdi_index` (float)
- `cdi_ret_t` (float, retorno diario)
- `source_tag` (string)

## Cobertura

- periodo_fonte: `2018-07-02` .. `2025-12-30`
- alvo_task: `2018-07-01` .. `2026-02-15`
- coverage_start_ok: `False`
- coverage_end_ok: `False`

## Metodo de alinhamento por pregao

A serie e mantida por `date` de pregao (uma linha por data observada na fonte), ordenada e deduplicada por data.

## Proveniencia e rastreabilidade

- ver `outputs/governanca/ssot_cdi/20260215/evidence/source_provenance.txt`
- ver `outputs/governanca/ssot_cdi/20260215/evidence/search_results.txt`
