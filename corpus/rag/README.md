# RAG Local

Modo `lessons` (padrao operacional):
- `min_score_lessons` padrao: `0.20`.
- resultados com `score == 0.0` sao descartados.
- se nenhum resultado atingir o limite, retorna `results: []` e `no_hits: true`.
- entradas com `external_ref=true` indicam evidencia fora do repo; `external_repo_hint` sugere a origem.

Modo `general`:
- busca em indice vetorial local com anti-ruido lexical e path boosts.

