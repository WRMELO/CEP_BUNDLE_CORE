# 1) Relatório de encerramento do ciclo M0…M6 (CEP_COMPRA v1)

Data de referência: 2026-02-13  
Bundle: CEP_COMPRA + CEP_NA_BOLSA  
Escopo encerrado: mecanismos M0, M1, M3, M4, M5, M6 e suas evoluções de controle no nível carteira.

## 1.1 Objetivo original do ciclo

Definir um mecanismo de compra (ranking e construção de carteira) maximizando retorno, mantendo:

- sistema defensivo diário de vendas (Master + Burners) como espinha dorsal;
    
- compra semanal (segunda-feira de manhã ou próximo pregão);
    
- alvo de 10 posições (faixa 8–12);
    
- universo via SSOT do bundle.
    

Ao longo do ciclo, ficou explícito que o problema central não era apenas “ranking”, mas o acoplamento entre:

- liberdade de exposição (tempo investido),
    
- gatilhos defensivos (no nível ativo e no nível carteira),
    
- e “path dependence” (se a carteira trava cedo, ela perde o compounding do bull market).
    

## 1.2 Linha evolutiva por mecanismo e o que cada um acrescentou

### M0 (baseline operacional de compra)

M0 serviu como referência inicial: compra com menos restrições e sem camadas novas de controle. Foi importante porque mostrou:

- que o sistema consegue operar e gerar uma trajetória plausível;
    
- mas que invariantes como “10 ± 1” não estavam sendo impostas como regra dura no runner.
    

### M1 (regras estruturais de compra + coerência de “upside exception” na venda)

M1 introduziu regras de qualidade/estrutura:

- filtro de volume (50/62);
    
- 1 classe por empresa;
    
- cap de setor (20%);
    
- mix B3/BDR (50%–80% B3);
    
- e alinhamento com a regra do Master: rompimento positivo (upside) não deve gerar SELL por si só.
    

O efeito observado foi típico: melhora muito em regime favorável, mas paga caro em regime adverso quando há giro alto e whipsaw (W2).

### M3 (M1 com score mais agressivo)

M3 combinou score e penalização (z-score + ret_62 − vol_62). O resultado ficou didático:

- ele captura muito em bull market (o “vai a 6” em W1),
    
- mas sofre drawdowns severos em regime adverso (W2/W3), incompatíveis com a intenção defensiva.
    

M3 tornou “visível” a tensão: compounding forte exige exposição; exposição sem freio gera drawdown profundo.

### M4 (primeiro motor defensivo explícito no nível carteira, mas com travamento)

M4 foi o primeiro mecanismo que implementou claramente:

- CEP da carteira em retorno diário (I-MR + run rules),
    
- máquina de estados (RISK_ON/OFF/HARD_PROTECTION),
    
- guardrail de HWM-10% com ação de proteção dura,
    
- bloqueio de BUY em estados defensivos.
    

O M4 cumpriu o guardrail por construção, mas ficou “morto” (absorvente em HARD_PROTECTION) e com caixa que não rendia como deveria. Serviu como prova de conceito do controle, mas não como estratégia operável.

### M5 (M4 corrigido: caixa rende CDI, downside-only para volatilidade, saída de proteção)

M5 corrigiu os defeitos mecânicos do M4:

- caixa passou a render CDI;
    
- “volatilidade com exceção de upside” (não derrubar estado em dia positivo);
    
- HARD_PROTECTION com regra de saída.
    

Isso tornou o sistema “vivo”, mas ainda muito conservador (exposição baixa e muitos dias em RISK_OFF). Além disso, o guardrail ainda estava implementado de forma não realista via “clamp”, o que exigiu a decisão C (realista).

### M6 (retorno ao realismo: guardrail sem clamp, CEP carteira por Xbarra-R, reposição controlada)

M6 implementou:

- CEP carteira por Xbarra-R (substituindo I-MR),
    
- disparos downside mais específicos (1 fora downside; 2 de 3 em 2σ negativo),
    
- reposição controlada em RISK_OFF (30% do caixa, até 5 posições estáveis),
    
- guardrail realista sem clamp (aceita overshoot por choque diário).
    

O resultado ficou coerente com defesa, mas confirmou o limite físico do modelo diário: “HWM-10% absoluto sem overshoot” não é garantível sem clamp ou sem modelagem intraday. O M6 também mostrou que limitar exposição (caixa alto + reposição limitada) inevitavelmente reduz a chance de “ir a 6” no mesmo horizonte.

## 1.3 Achados consolidados do ciclo

1. O “vai a 6” do M3 em W1 decorre de liberdade total de re-risking e alta exposição durante bull market. Não é só ranking; é regime + exposição.
    
2. Ao introduzir defesa no nível carteira (M4/M5/M6), a variável dominante passa a ser tempo investido e tamanho da exposição. Se o sistema trava cedo (por choque), ele perde o compounding mesmo que o ranking seja bom.
    
3. Guardrail HWM-10% tem duas interpretações:
    

- como bound matemático (clamp): cumpre sempre, mas não é execução real;
    
- como execução realista (sem clamp): reage após o fato, aceita overshoot em choques diários.
    

4. Usar “seleção de baseline pelo menor R/MR no histórico inteiro” é hindsight e não representa CEP operacional. Baseline de limites deve ser governado e ex-ante; a detecção de estado pode ser rolling e variável.
    
5. A conclusão conceitual do ciclo: Master não deve ser apenas um gate binário de compra; ele deve classificar regime (BULL/BEAR/TRANSIÇÃO), e cada regime deve ter política de carteira (níveis de liberdade de BUY), mantendo queimadores como critério individual por ativo para SELL.
    

## 1.4 Fragilidades identificadas (o que este ciclo não resolve)

- Incompatibilidade entre “capturar bull forte” e “nunca degradar pico” em modelo close-to-close sem intraday/derivativos.
    
- Sensibilidade de regras de carteira quando usadas como gate duro de compra (leva a baixa exposição).
    
- Falta de estrutura de avaliação com warmup governado (o que será resolvido no novo ciclo com 252 pregões).
    

## 1.5 Conclusão do encerramento

Este ciclo cumpriu sua função: expor, com evidência, que o problema principal não é “qual score compra”, e sim “como o Master define o regime e quanta liberdade de compra o portfólio tem em cada regime”, com baseline governado e avaliação ex-ante. A partir daqui, faz sentido iniciar um novo ciclo com:

- warmup fixo de 252 pregões,
    
- início operacional no pregão 253,
    
- Master como regime (BULL/BEAR/TRANSIÇÃO),
    
- políticas de BUY por regime (3 níveis),
    
- queimadores permanecendo como defesa individual por ativo.