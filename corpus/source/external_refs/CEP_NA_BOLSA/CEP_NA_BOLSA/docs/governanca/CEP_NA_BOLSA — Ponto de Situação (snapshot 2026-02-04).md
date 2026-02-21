According to a document from (2026-02-04), o “Planejamento Fase I” registrava (i) **Gate de Mercado = IBOVESPA**, (ii) **Master = IBOVESPA**, (iii) universo vindo da B3 e filtro por “recuperáveis”, e (iv) a variável operacional já pensada em **log-retorno** (em vez de preço bruto). Nesse mesmo trecho, a versão antiga ainda citava **yfinance/Yahoo** como fonte de preços e um recorte fixo em **2022** como referência inicial. A partir daí, o projeto evoluiu para o estado real abaixo (snapshot governado 2026-02-04).

---

# CEP_NA_BOLSA — Ponto de Situação (snapshot 2026-02-04)

Este snapshot lista apenas artefatos gerados por tasks Agno.

## 1. Constituição e governança (vigente)

A Constituição V2 foi publicada e apontada como “constituição vigente” via task de governança, com instrução JSON arquivada e evidência de execução (PASS). O repositório passou a refletir que:

- **“Universo”** vem de fonte institucional (B3/CVM).
    
- **“Dados de mercado”** vêm de provedor (BRAPI), para cotação/histórico.
    
- Decisões relevantes precisam estar em docs versionados no repo (Obsidian fora do Git).
    

Entregáveis já governados:

- `docs/CEP_NA_BOLSA_CONSTITUICAO_V2_20260204.md`
    
- `docs/governanca/constituição_vigente.md`
    
- `planning/agent_instructions/20260204/TASK_CEP_GOV_001_PUBLICAR_CONSTITUICAO_V2.instruction.json`
    

## 2. SSOTs institucionais de universo (B3) — concluídos

Foram concluídas (PASS) as tasks de SSOT de universo:

- **BDRs via B3** (XLSX raw + CSV/Parquet SSOT + manifesto + evidências).
    
- **Ações via B3** (JSON raw + CSV/Parquet SSOT + manifesto + evidências).
    

Isso fecha a separação arquitetural: **B3 = universo**; **BRAPI = mercado**.

## 3. SSOTs de dados de mercado (BRAPI) — preços brutos desde 2018-01-01

### 3.1 Ações (preços brutos)

Task concluída (PASS) coletando histórico bruto desde **2018-01-01**:

- Cobertura OK: **697 tickers**
    
- Falhas: **158 tickers** (classificadas depois; total 157 no classificador por deduplicação/normalização de chave)
    

Classificação de falhas (PASS):

- Total classificado: **157**
    
- `INVALID_TICKER`: **131**
    
- `DELISTED_OR_NO_HISTORY`: **26**
    
- Ação recomendada: `EXCLUDE_FROM_OPERATIONAL`
    

Um subconjunto crítico foi analisado: **NM/N2 ∩ falhas (40)** com dossiê e “root cause” (provedor não suporta ticker / instrumento não é ação à vista / mismatch de formato), deixando explícito o que faltava como metadado no SSOT (status negociável, etc.).

### 3.2 BDRs (preços brutos)

Task concluída (PASS) coletando histórico bruto desde **2018-01-01**:

- Cobertura OK: **650 tickers**
    
- Falhas: **188 tickers**
    

Classificação de falhas (PASS):

- Total: **188**
    
- Categoria dominante: `INVALID_TICKER (188)`
    
- Ação recomendada: `EXCLUDE_FROM_OPERATIONAL (188)`
    

Observação importante de consistência: pelos números reportados, o SSOT BDR total implícito é **838** (650 OK + 188 falhas). Para ações, o total implícito é **854** (697 OK + 157 excluídos).

## 4. Base operacional unificada (log-retorno) — concluída e auditada

Foi criada (PASS) a **base operacional XT unificada**, isto é, o banco operacional em **log-retorno** (ln(Close_t / Close_{t-1})) com governança e auditoria:

- Inputs: ações **1.205.324** linhas, BDR **1.017.225** linhas
    
- Tickers input: ações **697**, BDR **650**
    
- Excluídos: ações **157**, BDR **188**
    
- Resultado: **1.617.139** linhas, **1.242** tickers
    
- Janela final: **2018-01-03 até 2026-02-04**
    

## 5. Master / Gate de mercado: IBOVESPA (ticker ^BVSP) — SSOT separado e agregado

Atendendo ao combinado de “Master = IBOVESPA”, foi feita uma SSOT isolada do índice com BRAPI:

- Símbolo selecionado por probe: **^BVSP**
    
- Linhas preços: **2011**
    
- Linhas XT: **2010**
    
- Janela: **2018-01-02 até 2026-02-04**
    

Em seguida, o **XT do IBOV** foi agregado à base operacional:

- Linhas finais: **1.619.149**
    
- Tickers finais: **1.243**
    
- IBOV incluído como `asset_class=INDEX` e `ticker=^BVSP`
    

Até aqui, portanto, o “Gate de Mercado” (IBOV) está pronto para uso como **série Master**, sem criar um “Master artificial”: é o próprio índice.


---

# Lista inicial de eventos sistêmicos (marcadores primários) — datas de início

Objetivo destes marcadores: servir de “âncoras” externas reconhecidas, para avaliar se o CEP do Master (^BVSP em log-retorno) torna **visíveis** (i) choques de causa especial e (ii) possíveis mudanças sistêmicas. A partir deste snapshot, a definição de acerto passa a ser por **score de evento**, usando a melhor pontuação observada entre D, D+1, D+2, D+3 e D+5:

- D ou D+1: 100
- D+2: 80
- D+3 ou D+5: 50
- demais: 0

Score do evento = maior pontuação observada. Score do período = soma dos scores dos eventos.

Abaixo vai uma lista P0 (para já termos o conjunto-alvo). Ela será “governada” depois (task específica) com curadoria por bancos/corretoras/jornais e evidência cruzada.

1. 2018-05-21 — Início da paralisação/greve dos caminhoneiros (Brasil). ([Serviços e Informações do Brasil](https://www.gov.br/abin/pt-br/centrais-de-conteudo/noticias/retrospectiva-abin-25-anos-greve-dos-caminhoneiros-de-2018-aperfeicoou-o-acompanhamento-de-inteligencia-corrente?utm_source=chatgpt.com "Retrospectiva ABIN 25 anos: greve dos caminhoneiros de ..."))
    
2. 2020-03-09 — Primeiro circuit breaker do Ibovespa em 2020 (choque inicial de COVID/mercados). ([Reuters](https://www.reuters.com/article/world/americas/ibovespa-cai-10-e-aciona-circuit-breaker-com-nervosismo-global-petrobras-derre-idUSKBN20W26R/?utm_source=chatgpt.com "Ibovespa cai 10% e aciona circuit breaker com nervosismo ..."))
    
3. 2020-03-11 — OMS declara COVID-19 como pandemia (marco global). ([Organização Mundial da Saúde](https://www.who.int/news-room/speeches/item/who-director-general-s-opening-remarks-at-the-media-briefing-on-covid-19---11-march-2020 "WHO Director-General's opening remarks at the media briefing on COVID-19 - 11 March 2020"))
    
4. 2021-03-17 — Copom inicia ciclo de alta (Selic para 2,75% a.a.; 1ª alta em anos). ([Banco Central do Brasil](https://www.bcb.gov.br/detalhenoticia/17341/nota?utm_source=chatgpt.com "Copom eleva a taxa Selic para 2,75% a.a."))
    
5. 2022-02-24 — Início da invasão em larga escala da Ucrânia pela Rússia (marco geopolítico global). ([The United Nations Office at Geneva](https://www.ungeneva.org/en/news-media/news/2024/11/100447/un-underlines-solidarity-ukraine-1000-days-russian-invasion?utm_source=chatgpt.com "UN underlines solidarity with Ukraine 1,000 days into Russian ..."))
    
6. 2022-03-16 — FOMC eleva a meta dos Fed Funds (início do ciclo 2022 de alta nos EUA). ([Reserva Federal](https://www.federalreserve.gov/newsevents/pressreleases/monetary20220316a.htm?utm_source=chatgpt.com "Federal Reserve issues FOMC statement"))
    
7. 2022-10-30 — 2º turno das eleições presidenciais no Brasil (marco político doméstico). ([Justiça Eleitoral](https://www.tse.jus.br/comunicacao/noticias/2022/Outubro/lula-e-eleito-novamente-presidente-da-republica-do-brasil "Lula é eleito novamente presidente da República do Brasil — Tribunal Superior Eleitoral"))
    
8. 2023-01-08 — Ataques/invasões às sedes dos Três Poderes (marco institucional doméstico). ([Senado Federal](https://www12.senado.leg.br/noticias/materias/2024/01/05/ataques-de-8-de-janeiro-tiveram-reflexo-na-agenda-legislativa-em-2023?utm_source=chatgpt.com "Ataques de 8 de janeiro tiveram reflexo na agenda ..."))
    
9. 2023-03-10 — Fechamento do Silicon Valley Bank (stress bancário/risco sistêmico). ([FDIC](https://www.fdic.gov/news/press-releases/2023/pr23019.html?utm_source=chatgpt.com "FDIC Acts to Protect All Depositors of the former Silicon ..."))
    
10. 2023-03-19 — Anúncio do acordo UBS–Credit Suisse (stress bancário/Europa). ([United States of America](https://www.ubs.com/global/en/media/display-page-ndp/en-20230319-tree.html?utm_source=chatgpt.com "UBS to acquire Credit Suisse"))
    

(Se você quiser, ainda dentro de “marcadores primários”, dá para incluir também datas de 2018/2022 em dois níveis: “evento (pleito)” e “data de resultado oficial” — mas eu mantive aqui os marcos mais consensuais e diretamente datáveis.)

---

# Emenda proposta à Constituição (rascunho) — definição do período de referência do CEP do Master

Você pediu apenas que conste que o período de referência virá de **regras conhecidas**, aplicadas ao **Master (IBOV)**. Segue um texto pronto para entrar como emenda (sem alterar o restante):

**Emenda — Regra de definição do período de referência (baseline) do CEP do Master**  
O período de referência do CEP (baseline) será definido formalmente por regras determinísticas, aplicadas exclusivamente à série Master (IBOVESPA, em log-retorno). O baseline deve ser um intervalo contínuo de pregões, contido na janela de dados disponível (a partir de 2018-01-01), com tamanho mínimo pré-definido (em pregões) e com dados íntegros. A escolha do baseline será feita por um procedimento de seleção comparativa entre candidatos, usando (i) uma lista governada de eventos sistêmicos com datas de início e (ii) métricas objetivas de desempenho do CEP no Master.

Definição de acerto por score de evento: D ou D+1 = 100; D+2 = 80; D+3 ou D+5 = 50; demais = 0. Para cada evento, usa-se o maior score observado; o score do período é a soma dos scores dos eventos. Em caso de empate de score total entre períodos candidatos, escolher o período com **menor número de sessões**. A seleção do período não se ancora em convenções econômicas (ex.: 252 pregões), mas em número de sessões e critérios estatísticos do CEP.

Procedimento: usar carta Xbarra–R no Master variando N (subgrupo), começando em N=2; selecionar o melhor período por score; congelar o período; aumentar N (3, 4, ...) e verificar se melhora o score. Eventos sistêmicos são referência ex post para auditoria/seleção e não alteram a lógica CEP do monitoramento.

## Fase de Teste — Início

Método adotado: N=2 -> escolher período por score -> congelar período -> aumentar N e comparar score.

Parâmetros desta execução:

- baseline: min_sessoes=60; max_sessoes=400; comprimentos_testados=[60,80,100,120,160,200,260,320,400]; estrategia=janela_deslizante; passo_sessoes=5.
- scan N: N_max=10.

## Ponto de Situação — Pós-Teste

- Período vencedor: 2024-02-29 até 2024-05-24 (60 sessões)
- N vencedor: 4
- Score total: 340

### Score por evento

| event_date | score |
| --- | --- |
| 2018-05-21 | 0 |
| 2020-03-09 | 80 |
| 2020-03-11 | 100 |
| 2021-03-17 | 0 |
| 2022-02-24 | 0 |
| 2022-03-16 | 80 |
| 2022-10-30 | 0 |
| 2023-01-08 | 80 |
| 2023-03-10 | 0 |
| 2023-03-19 | 0 |

### Score por N (baseline congelado)

| n | score_total |
| --- | --- |
| 4 | 340 |
| 3 | 230 |
| 5 | 230 |
| 6 | 180 |
| 9 | 180 |
| 10 | 180 |
| 7 | 100 |
| 8 | 0 |

### Evidências e artefatos

- outputs/master_baseline_selection/20260204/baseline_candidates_manifest.json
- outputs/master_baseline_selection/20260204/results_N2.parquet
- outputs/master_baseline_selection/20260204/selected_baseline_N2.json
- outputs/master_baseline_selection/20260204/results_N_scan.parquet
- outputs/master_baseline_selection/20260204/selected_N_master.json

## Reaplicação do Teste (N fixo = 4, varredura de comprimentos 10..300 passo 10)

- Baseline vencedor: 2024-08-06 até 2024-08-19 (10 sessões)
- Score total: 570

### Score por comprimento (N=4)

| n_sessoes | score_total |
| --- | --- |
| 10 | 570 |
| 20 | 550 |
| 30 | 470 |
| 40 | 420 |
| 50 | 420 |
| 60 | 420 |
| 70 | 420 |
| 80 | 340 |
| 90 | 340 |
| 100 | 340 |
| 110 | 340 |
| 120 | 340 |
| 130 | 340 |
| 140 | 340 |
| 150 | 340 |
| 160 | 340 |
| 170 | 340 |
| 180 | 340 |
| 190 | 340 |
| 200 | 340 |
| 210 | 340 |
| 220 | 340 |
| 230 | 340 |
| 240 | 340 |
| 250 | 340 |
| 260 | 340 |
| 270 | 340 |
| 280 | 340 |
| 290 | 260 |
| 300 | 260 |

### Evidências e artefatos

- outputs/master_baseline_selection/20260204/baseline_candidates_len_scan_N4_manifest.json
- outputs/master_baseline_selection/20260204/results_len_scan_N4.parquet
- outputs/master_baseline_selection/20260204/selected_baseline_N4.json

## Decisão de Freeze do Master

- Calibração congelada: baseline 2024-02-29 → 2024-05-24 (60 sessões), N=4
- Fonte de decisão: `TASK_CEP_F1_006_MASTER_BASELINE_N_SELECTION_V1`
- SSOT: `docs/ssot/master_calibration.json`
