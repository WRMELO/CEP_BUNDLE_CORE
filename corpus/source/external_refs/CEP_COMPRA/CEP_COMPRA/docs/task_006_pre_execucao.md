# TASK_CEP_COMPRA_006 - Pre-execucao

## Mapeamento de colunas (portfolio_daily.parquet)

Arquivo:
- `/home/wilson/CEP_COMPRA/outputs/reports/task_005/run_20260211_211715/data/portfolio_daily.parquet`

Schema identificado:
- `date` (data)
- `position_value_brl` (valor por ativo no dia)
- `cash_brl` (caixa)
- `equity_brl` (equity reportada)
- `ticker`, `weight`, `n_positions`

Mapeamento aplicado:
- Date: `date`
- Equity: `equity_brl`
- Cash: `cash_brl`
- Soma de posicoes: agregacao diaria de `position_value_brl`

Cheque de consistencia:
- `equity_brl` vs `sum(position_value_brl) + cash_brl`
- max abs diff observado: ~`5.82e-11`
- conclusao: `equity_brl` ja inclui caixa; serie `carteira_total_brl` usara `equity_brl`.

## Fontes de benchmark registradas

- CDI (preferencial web/SGS Bacen):
  - `https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=02/07/2018&dataFinal=31/12/2025`
- S&P 500 (fallback web confiavel):
  - `https://stooq.com/q/d/l/?s=%5Espx&i=d`

## Regra de alinhamento

- Calendario de referencia: datas da carteira.
- Benchmarks (CDI e S&P 500): `forward-fill` em dias sem atualizacao.
- Se houver `NaN` apenas no inicio, aplicar `backward-fill` para completar base.
- Normalizacao final: base 1.0 no primeiro dia da carteira no range.
