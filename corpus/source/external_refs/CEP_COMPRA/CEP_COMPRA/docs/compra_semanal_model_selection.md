# Compra semanal: critérios, acoplamento com venda diária e seleção do mecanismo por dados

## 1. Contexto e objetivo
Este projeto tem escopo restrito: definir e validar o mecanismo de COMPRA semanal para uma carteira de ações e BDRs com viés defensivo, mantendo a rotina diária de VENDA já congelada como mecanismo de proteção e disciplina operacional.

A premissa é separar responsabilidades:
- Rotina diária de VENDA: congelada, governada, tratada como dependência estável.
- Rotina semanal de COMPRA: orientada a priorização de rendimento, com seleção do mecanismo guiada por dados (jan/2018 até hoje) e com regras reprodutíveis.

A integração com a rotina diária de venda ocorrerá apenas após a definição do mecanismo de compra, com evidências e artefatos Agno.

## 2. Decisões congeladas

### 2.1. Elegibilidade por CEP (binária)
Foi congelada a decisão de usar CEP exclusivamente como elegibilidade binária, com dois estados:
- Em controle
- Fora de controle.

A avaliação de elegibilidade é feita na última janela que atende:
- K = 60 subgrupos,
- n = 3,
- equivalente operacional: 62 sessões para compor a avaliação mais recente.

Os limites/estatísticas usados são os calculados no baseline e aplicados aos ativos. A avaliação do estado do ativo é sempre sobre a janela mais recente disponível (K=60, n=3).

### 2.2. Rotina diária de VENDA permanece fixa
Este projeto não altera a rotina diária de venda. A compra semanal deve encaixar com esse mecanismo e aceitar como dados:
- vendas diárias/semanais segundo critérios congelados;
- caixa resultante disponível;
- restrições de concentração e guardrails definidos na Constituição (não redefinir neste projeto).

### 2.3. Disponibilidade de dados
Há dados desde janeiro de 2018 até a data atual, permitindo avaliação em múltiplos regimes de mercado.

## 3. Baseline conceitual de compra semanal

### 3.1. Ranking baseline
Proposta inicial: ranquear todos os ativos elegíveis (em controle pelo CEP) pela média do retorno no período K, do maior para o menor.

O ranking considera conjuntamente:
- ativos já presentes na carteira;
- ativos prospectivos (fora da carteira), desde que elegíveis pelo CEP.

### 3.2. Regras baseline de uso do caixa (processo sequencial)
Dado o caixa disponível, percorre-se o ranking do primeiro ao último, aplicando a regra apropriada, até o caixa acabar:

1) Se nas primeiras posições existirem ativos que já estão na carteira:
- usar caixa para completar até o limite máximo de participação na carteira, conforme definido na Constituição.

2) Se nas primeiras posições só houver ativos que não pertencem à carteira:
- usar caixa para comprar o primeiro até atingir 10% de participação (ou menos se o caixa não permitir),
- prosseguir para o segundo, sucessivamente, até o caixa acabar.

3) Se houver alternância entre ativos presentes e não presentes na carteira:
- alternar as regras (1) e (2) conforme a posição do ranking, até o limite do caixa informado.

As regras anti-concentração já existem na Constituição e não devem ser reescritas aqui.

## 4. Diretriz estratégica: seleção do mecanismo de compra guiada por dados

### 4.1. Motivação
Definir percentuais, faixas e cortes ex ante tende a ser arbitrário e teórico. A decisão proposta é usar os dados históricos (2018→hoje) para selecionar, por desempenho, qual mecanismo de compra semanal é superior, mantendo o motor diário de venda congelado.

### 4.2. Formulação como seleção de mecanismo de compra
O problema será tratado como seleção entre mecanismos candidatos de compra semanal (cada um versionado e definido de forma reprodutível).

Em cada semana do histórico:
- calcula-se elegibilidade CEP (em controle / fora de controle),
- forma-se o conjunto elegível,
- aplica-se o mecanismo de compra candidato para gerar ordens de compra usando o caixa disponível e restrições da Constituição,
- ao longo da semana, a rotina diária de venda atua de forma fixa,
- mede-se o desempenho conforme a função de avaliação definida (incluindo custos e penalizações).

Ao final, compara-se o desempenho acumulado e por regimes, e decide-se o mecanismo vencedor com evidências.

### 4.3. Penalizações assimétricas
A avaliação deve incluir penalização:
- quando compra errado,
- penalização maior quando deixa de comprar certo.

Para operacionalizar “deixar de comprar certo”, será necessário definir uma regra de avaliação ex post (convenção mensurável e reprodutível) dentro do protocolo de backtest.

## 5. Protocolo de validação

### 5.1. Separação temporal (walk-forward)
A validação deve respeitar causalidade:
- seleção/ajuste usando passado,
- avaliação em janelas futuras (walk-forward),
- período final reservado para validação final out-of-sample.

### 5.2. Venda diária fixa em todos os testes
Em todos os testes, a rotina diária de venda deve ser a mesma, para isolar o efeito do mecanismo de compra semanal.

## 6. Artefatos esperados (Agno)
Este projeto deverá produzir artefatos reprodutíveis e auditáveis:
1) Definições formais dos mecanismos candidatos de compra semanal (versionadas).
2) Definição formal da função de avaliação (reward) e das penalizações assimétricas.
3) Relatórios de backtest walk-forward comparando mecanismos.
4) Decisão final do mecanismo escolhido, com evidências.
5) Artefato do mecanismo vencedor (policy/mecanismo) pronto para integração posterior ao pipeline de venda diária.

## 7. Interface para integração futura
A integração futura deve preservar acoplamento mínimo:

Entradas para compra semanal:
- conjunto elegível (CEP em controle),
- estado atual (carteira, caixa, restrições da Constituição),
- outputs da rotina diária (incluindo vendas e caixa atualizado).

Saídas da compra semanal:
- ordens de compra (e eventualmente metas/pesos), a serem consumidas pela execução.

