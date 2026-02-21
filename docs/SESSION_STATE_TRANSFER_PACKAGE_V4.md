# 📦 SESSION_STATE_TRANSFER_PACKAGE_V4.md

*(CEP_BUNDLE_CORE — Pós RAG v7 — Início Tentativa 2)*

## 1. Estado Geral do Projeto

Repositório ativo:
`/home/wilson/CEP_BUNDLE_CORE`

Tentativa ativa de reexecução:
`/home/wilson/CEP_BUNDLE_CORE/_tentativa2_reexecucao_completa_20260220`

RAG ativo:

* index_version: **v7**
* index_path: `corpus/rag/index/index_v7.json`
* KB: `corpus/lessons/LESSONS_LEARNED.json`
* Total de lições: **124**
* Modo lessons endurecido (min_score=0.20, no padding, no_hits=true)

---

## 2. Estado do RAG / Corpus

### Versão atual

* Manifest: `corpus_manifest_v7.json`
* Index: `index_v7.json`
* Corpus autocontido (external_ref_entries=0)
* Anchors preservadas (TASK 009)
* Cost model seeded (TASK 010)

### Lições críticas ativas

* `LL-20260220-900` — PASS contestado exige reconciliação econômica
* `LL-20260220-901` — Divergência numérica = quebra de consistência
* `LL-20260220-COST-001` — Definição normativa do custo 0,025%
* `LL-20260220-COST-002` — Custo sobre abs(notional) e impacto em turnover

Consulta validada:

```
collection=lessons
query="custo 0,025%"
→ 2 resultados (não é mais no_hits)
```

---

## 3. Situação Estratégica Atual

Problema central:

> Após inúmeras tentativas, não se conseguiu melhorar M3.
>
> * Em W1, M3 chegou a ~6x.
> * Em W2, caiu para ~2,3x.
> * A hipótese levantada foi "deriva".
> * Testes posteriores não resolveram.

Agora o foco mudou completamente.

---

## 4. Nova Direção Definida

### Foco exclusivo:

**Analisar M3 apenas no período W1.**

Objetivo:

1. Reexecutar M3 no período W1.
2. Entender por que ocorreram vendas:

   * Qual regra disparou?
   * Estava em downside?
   * Qual regime de mercado?
   * Volatilidade?
   * Turnover?
3. Separar análise por trimestres.
4. Usar notação diferenciada.
5. Depositar toda análise dentro da Tentativa 2.
6. Não mexer no snapshot.

---

## 5. Estado Técnico da Tentativa 2

Diretório base:

```
/home/wilson/CEP_BUNDLE_CORE/_tentativa2_reexecucao_completa_20260220
```

Estrutura existente:

* `ssot_snapshot/` → congelado
* `work/` → área de execução
* `outputs/` → evidências

Regra operacional:

* Snapshot não pode ser alterado.
* Todo artefato novo deve nascer dentro de `work/` ou `outputs/`.

---

## 6. O Que Ainda NÃO Está Congelado

Precisam ser encontrados nos documentos (não perguntados ao usuário):

1. Datas exatas do período W1.
2. Definição exata de M3 em W1 (CEP puro 2 critérios? outra versão?).
3. Regra(s) de venda ativas naquele período.
4. Se custo 0,025% já estava ativo em W1.

Sem isso não se pode reexecutar.

---

## 7. Notação Nova para a Análise

Para evitar contaminação com versões posteriores:

* ( M3^{W1}_{orig} ) → modelo original W1
* ( M3^{W1}_{re} ) → reexecução atual
* ( V_i ) → evento de venda i
* ( R(V_i) ) → regra disparadora
* ( \Sigma_t ) → regime de volatilidade
* ( D_t ) → condição de downside
* ( T_q ) → trimestre q

---

## 8. Próxima Etapa Esperada

Criar task para:

* Localizar e congelar definições W1.
* Reexecutar M3 no período W1.
* Extrair todos os eventos de venda.
* Produzir relatório por trimestre.
* Identificar padrão estrutural.

---

# ✅ Conclusão

O chat está consistente e não corrompido.
Mas, dado o volume técnico já acumulado, **eu recomendo abrir um novo chat agora**, colando apenas o V4.

Isso elimina ruído, acelera processamento e mantém rastreabilidade limpa.

Se você quiser, eu já preparo a primeira instrução do novo chat para iniciar diretamente pela TASK de congelamento de W1.
