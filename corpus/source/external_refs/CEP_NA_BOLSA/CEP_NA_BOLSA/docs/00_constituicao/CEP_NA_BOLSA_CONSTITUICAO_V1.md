

## Documento de Abertura e Diretrizes Operacionais

### 1. Propósito do Projeto

O projeto **CEP_NA_BOLSA** tem como objetivo desenvolver um sistema de gestão e operação de ativos financeiros baseado **exclusivamente em Controle Estatístico de Processo (CEP)**, aplicando princípios clássicos de engenharia de processos (Western Electric / Nelson Rules) ao mercado de ações e BDRs.

O foco do projeto **não é previsão**, **não é narrativa de mercado** e **não é otimização prematura**, mas sim:

- operar **somente quando o processo estiver sob controle**;
    
- bloquear risco quando houver evidência estatística de causa especial;
    
- selecionar ativos com base em **estabilidade + resultado**, sempre condicionados ao CEP;
    
- manter **auditabilidade total** das decisões.
    

O projeto é **pessoal**, **experimental** e **engenheirado**, sem compromissos com práticas comerciais de mercado.

---

### 2. Filosofia Central

- O mercado é tratado como **processo estocástico com rupturas**, não como sistema causal previsível.
    
- O CEP é a **camada primária de decisão**, acima de qualquer modelo ou ranking.
    
- Nenhuma decisão operacional é tomada fora das regras CEP formalmente definidas.
    
- Toda sofisticação futura (ML, fatores, etc.) só pode existir **dentro de um regime estatisticamente sob controle**.
    

Frase-guia do projeto:

> _“Não operar processos fora de controle.”_

---

### 3. Arquitetura de Papéis (Obrigatória)

O projeto adota explicitamente a seguinte separação de responsabilidades:

- **Owner (Você)**  
    Responsável por objetivos, validações finais, decisões estratégicas e aceitação dos artefatos.
    
- **Planejador (ChatGPT – este papel)**  
    Responsável por:
    
    - definir constituições, regras e planos de trabalho;
        
    - manter coerência metodológica;
        
    - evitar improvisação;
        
    - escrever documentos estruturantes;
        
    - nunca “pular etapas”.
        
- **Orquestrador (Agno)**  
    Responsável por:
    
    - organizar tarefas em etapas claras;
        
    - garantir rastreabilidade;
        
    - executar planos aprovados;
        
    - registrar logs e resultados.
        
- **Agente (Cursor + modelo de código)**  
    Responsável por:
    
    - implementação técnica (código);
        
    - execução fiel das tarefas definidas;
        
    - não tomar decisões conceituais.
        

⚠️ **Regra de ouro**:  
O Planejador **não codifica**, o Agente **não decide**, o Orquestrador **não interpreta**.

---

### 4. Ferramentas Oficiais do Projeto

Estas ferramentas fazem parte **formal** do projeto e devem continuar sendo usadas:

- **Agno**
    
    - Organização do trabalho em tarefas.
        
    - Execução ordenada e rastreável.
        
    - Registro de resultados intermediários.
        
- **Cursor**
    
    - Implementação técnica.
        
    - Execução de código.
        
    - Interface prática com o Agno.
        
- **Obsidian**
    
    - Registro histórico.
        
    - Decisões importantes.
        
    - Versões conceituais da constituição.
        
    - Diário do projeto.
        
- **Mermaid** (Obsidian ou online)
    
    - Fluxos operacionais.
        
    - Sequência de decisão CEP.
        
    - Estados do Master e dos Queimadores.
        
- **Miro**
    
    - Mapas mentais.
        
    - Exploração conceitual.
        
    - Organização de ideias antes de formalização.
        

Nenhuma ferramenta substitui outra. Cada uma tem papel distinto.

---

### 5. Estrutura Conceitual do Sistema

O sistema é dividido em **duas camadas principais**, ambas regidas por CEP clássico:

#### 5.1 Master (Gate Master)

- Representa o **regime agregado do mercado**.
    
- Decide:
    
    - se compras são permitidas;
        
    - se o sistema entra em modo de preservação.
        
- Não seleciona ativos.
    
- Usa cartas CEP (Xbarra–R, I, MR) sobre **log-retorno**.
    

#### 5.2 Queimadores (Ativos Individuais)

- Cada ativo é tratado como um **processo independente**.
    
- Decide:
    
    - manter;
        
    - reduzir;
        
    - zerar;
        
    - tornar-se elegível novamente.
        
- Não há quarentena fixa arbitrária.
    
- Reentrada ocorre **quando o processo volta a estar sob controle estatístico**.
    

⚠️ Apenas regras CEP clássicas são permitidas para declarar “fora de controle”.

---

### 6. Dados e Variável Fundamental

- Fonte de preços: **yfinance**.
    
- Universo: ações e BDRs disponíveis (com saneamento mínimo).
    
- Variável única do projeto:
    
    [  
    X_t = \log\left(\frac{Close_t}{Close_{t-1}}\right)  
    ]
    

Nenhuma outra variável substitui esta para fins de controle.

---

### 7. Calibração e Fases do Projeto

O projeto segue rigorosamente a lógica CEP:

- **Fase I (Calibração)**
    
    - Ano base: **2022**.
        
    - Objetivo:
        
        - definir (n_{burner}), (n_{master}) e persistências (k);
            
        - estimar limites de controle;
            
        - avaliar estabilidade do controlador (não performance financeira).
            
- **Fase II (Aplicação)**
    
    - Período: **2023 até janeiro/2026**.
        
    - Objetivo:
        
        - observar comportamento do sistema;
            
        - validar decisões do Gate Master;
            
        - analisar entradas e saídas dos queimadores.
            

Nenhum parâmetro é ajustado em Fase II sem revisão formal da constituição.

---

### 8. Seleção de Ativos para Compra (Condicionada ao CEP)

Quando o Gate Master permitir compras, a seleção segue este princípio:

- Universo elegível = ativos **em controle estatístico**.
    
- Avaliação periódica (semanal):
    
    - média de log-retorno em janela (t);
        
    - medida de estabilidade (amplitude ou desvio-padrão).
        
- Classificação em quadrantes teóricos:
    
    - alta média + baixa dispersão = prioridade.
        
- Alocação de capital:
    
    - respeitando limites de carteira (ex.: máx. 20% por ativo);
        
    - respeitando diversidade de mercado.
        

⚠️ Métricas de ranking **não substituem** CEP.  
CEP define _se_ pode operar; ranking define _como_ operar.

---

### 9. Princípios de Qualidade e Disciplina

- Nenhuma regra implícita.
    
- Nenhuma decisão sem log.
    
- Nenhum “jeitinho” fora da constituição.
    
- Nenhuma sofisticação antes de controle estável.
    
- Toda mudança conceitual exige atualização formal deste documento.
    

---

### 10. Instrução ao Próximo Chat (Explícita)

> **Este chat inicia o projeto CEP_NA_BOLSA.**
> 
> O Planejador deve:
> 
> - respeitar integralmente esta constituição;
>     
> - começar pelo plano técnico de execução da Fase I;
>     
> - organizar o trabalho via Agno;
>     
> - não antecipar ML, previsão ou otimizações;
>     
> - priorizar clareza, rastreabilidade e controle estatístico.
>     

---

### Encerramento

Este documento é a **âncora conceitual** do projeto CEP_NA_BOLSA.  
Tudo o que vier depois deve ser compatível com ele — ou explicitamente revisado.

---

