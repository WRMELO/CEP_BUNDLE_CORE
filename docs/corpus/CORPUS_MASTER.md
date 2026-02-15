# CORPUS_MASTER - Conhecimento legado consolidado

## Escopo de origem
- `/home/wilson/CEP_NA_BOLSA`
- `/home/wilson/CEP_COMPRA`

## Cobertura de varredura
- `/home/wilson/CEP_NA_BOLSA/docs` -> `scanned`
- `/home/wilson/CEP_NA_BOLSA/planning` -> `scanned`
- `/home/wilson/CEP_NA_BOLSA/outputs` -> `scanned`
- `/home/wilson/CEP_COMPRA/docs` -> `scanned`
- `/home/wilson/CEP_COMPRA/planning` -> `scanned`
- `/home/wilson/CEP_COMPRA/outputs` -> `scanned`

- Arquivos varridos: **1180**
- Experimentos/identificadores catalogados: **27**
- Artefatos referenciados: **7130** (existentes: **7120**; hasheados: **400**)

## Experimentos identificados (amostra)
- `EXP_001`: fontes=1, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_001_CTRL_RL_MODE_LAYER_V1`: fontes=14, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_002A`: fontes=2, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_002A_MASTER_REGIME_V4`: fontes=18, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_002A_MASTER_REGIME_V4C`: fontes=34, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_002B`: fontes=1, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_002B_MASTER_REGIME_SUPERVISED_PRICE_THEORY_V1`: fontes=23, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_003A`: fontes=1, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_003A_MASTER_REGIME_V6_4STATE`: fontes=31, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_003B`: fontes=1, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_003B_MASTER_REGIME_V7_5STATE`: fontes=24, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_003C`: fontes=2, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_003C_MASTER_BUY_SSOT_V8`: fontes=26, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_020`: fontes=3, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_029`: fontes=6, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_031`: fontes=4, métricas=0, resultados=0, CEP=0, forense=0
- `EXP_032`: fontes=3, métricas=0, resultados=0, CEP=0, forense=0
- `GERAL_CEP_REFERENCIAS`: fontes=0, métricas=0, resultados=0, CEP=120, forense=0
- `GERAL_FORENSE_REFERENCIAS`: fontes=0, métricas=0, resultados=0, CEP=0, forense=120
- `M0`: fontes=120, métricas=24, resultados=24, CEP=2, forense=6
- `M1`: fontes=120, métricas=26, resultados=28, CEP=0, forense=7
- `M3`: fontes=120, métricas=15, resultados=8, CEP=120, forense=24
- `M4`: fontes=120, métricas=1, resultados=0, CEP=108, forense=0
- `M5`: fontes=103, métricas=1, resultados=0, CEP=0, forense=0
- `M6`: fontes=120, métricas=1, resultados=0, CEP=3, forense=1
- `W1`: fontes=120, métricas=3, resultados=1, CEP=119, forense=11
- `W2`: fontes=120, métricas=4, resultados=4, CEP=109, forense=10

## Métricas e resultados quantitativos
- O corpus captura linhas de métricas (multiplo, MDD, drawdown, recuperação, retorno, sharpe, CAGR etc.) por experimento.
- Os resultados quantitativos detectados ficam em `docs/corpus/experimentos.json` no campo `quant_results`.

## Uso de CEP/SPC
- Menções a CEP/SPC, Xbarra/Xbar, I-MR, Western/Nelson e controle estatístico foram mapeadas por experimento e em referências gerais.

## Lições aprendidas
- Trechos com padrão de lição/recomendação/conclusão foram consolidados em `docs/corpus/licoes_aprendidas.json`.

## Artefatos técnicos referenciados
- Caminhos citados nos legados foram catalogados com existência e hash quando aplicável em `docs/corpus/artefatos_referenciados.json`.

## Validação interna
- Referências marcadas como existentes/ausentes.
- Índice em `docs/corpus/corpus_index.json`.
