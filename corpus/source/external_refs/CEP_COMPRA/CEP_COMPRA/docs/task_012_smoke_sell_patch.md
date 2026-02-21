# Task 012 - Smoke test do patch de venda (excecao de upside)

## Regra testada

- `upside_extreme = (xbar > ucl_xbar) OR (xt > ucl_i)`
- SELL por rompimento positivo deve ser bloqueado quando `upside_extreme=True`.
- `stress_amp = (r > ucl_r) AND NOT(upside_extreme)`.

## Cenarios sinteticos executados

Resultado observado:

```csv
case,upside_extreme,stress_amp,sell
I_UCL_upside,True,False,False
MR_UCL_sem_upside,False,False,True
R_UCL_com_upside,True,False,False
```

Interpretação:

- `I_UCL_upside`: com `xt > ucl_i` e `upside_extreme=True`, nao vende.
- `MR_UCL_sem_upside`: com `mr > mr_ucl` e sem upside, vende.
- `R_UCL_com_upside`: mesmo com `r > ucl_r`, se upside ativo, `stress_amp=False` e nao vende.

## Arquivos de código afetados

- `tools/task_004_backtest_runner.py`
- `tools/task_005_report_runner.py`
- `tools/task_012_runner.py`
