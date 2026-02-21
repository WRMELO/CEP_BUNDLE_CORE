# Protocolo Operacional de Modelos

## 1. Objetivo

Estabelecer uma regra fixa de operação entre Owner, planejadores, executor e orquestrador para garantir:

- decisões não técnicas sob autoridade do Owner;
- execução técnica rastreável e reproduzível;
- comparação objetiva entre planejador principal e planejador candidato.

Este protocolo vale para o projeto `CEP_COMPRA`.

## 2. Papéis e responsabilidades

- **Owner (Wilson)**  
  Autoridade final para toda decisão não técnica: estratégia, escopo, priorização, critérios de negócio, aceitação final.

- **Planejador principal (GPT 5.2 Thinking externo)**  
  Responsável por discussão técnica/estratégica e proposta principal de direcionamento.

- **Planejador candidato (Opus 4.6 interno no Cursor)**  
  Responsável por proposta alternativa/comparativa para teste controlado de qualidade.

- **Executor (GPT 5.3 Codex)**  
  Implementa tarefas aprovadas, sem decidir estratégia não técnica.

- **Orquestrador (Agno)**  
  Executa tasks, aplica gates, registra evidências, logs e `report.json`.

## 3. Regras mandatórias

1. Nenhuma decisão não técnica entra em execução sem aprovação explícita do Owner.
2. Execução ocorre somente com instrução estruturada (task JSON ou especificação equivalente aprovada).
3. Toda task deve ter critérios de PASS/FAIL e outputs verificáveis.
4. Em caso de ambiguidade, o executor deve parar e perguntar ao Owner.
5. Sem evidência rastreável, a task não é considerada concluída.

## 4. Fluxo operacional (fim a fim)

1. **Planejamento inicial no Cursor (Opus):** Opus 4.6 interno abre a proposta no escopo da task.
2. **Planejamento principal (5.2 externo):** 5.2 propõe estratégia/task para comparação.
3. **Decisão do Owner:** escolhe 5.2, Opus, ou síntese.
4. **Execução técnica:** 5.3 executa exatamente o aprovado.
5. **Orquestração e registro:** Agno gera evidências e status final.
6. **Pós-task:** registrar comparação 5.2 vs Opus (curta e objetiva).

## 5. Gates de governança

- **GATE 1 — Escopo aprovado**  
  O escopo está explícito e aprovado pelo Owner?

- **GATE 2 — Entradas canônicas**  
  Paths, ponteiros `latest` e fontes de verdade estão definidos?

- **GATE 3 — Critérios objetivos**  
  PASS/FAIL, outputs e validações estão definidos antes de executar?

- **GATE 4 — Rastreabilidade**  
  A execução gerou logs/evidências/report?

- **GATE 5 — Aceite do Owner**  
  Resultado final apresentado e aceito pelo Owner?

## 6. Critérios de comparação (5.2 vs Opus)

Para cada task relevante, avaliar:

- clareza da especificação;
- aderência à governança;
- nível de retrabalho exigido;
- tempo de ciclo até aceite;
- qualidade dos riscos e guardrails identificados.

## 7. Regra de desempate

- Em divergência entre planejadores, prevalece:
  1) decisão explícita do Owner;
  2) se não houver decisão ainda, manter proposta do planejador principal (5.2) até deliberação do Owner.

## 8. Comunicação mínima obrigatória por task

Antes da execução:

- identificação explícita do LLM na primeira linha da mensagem (formato: `[LLM: <modelo/agente>]`);
- objetivo;
- constraints;
- inputs obrigatórios;
- outputs esperados;
- critérios de PASS/FAIL;
- pontos que exigem decisão do Owner.

Após execução:

- identificação explícita do LLM na primeira linha da mensagem (formato: `[LLM: <modelo/agente>]`);
- overall PASS/FAIL;
- step PASS/FAIL;
- arquivos criados/alterados;
- riscos/lacunas pendentes.

Regra operacional no Cursor:

- toda mensagem operacional deve começar com identificação do LLM autor;
- na ausência dessa identificação, a mensagem é considerada inválida para governança.

## 9. Política de mudança de chat/projeto

Em cada novo chat:

- reafirmar papéis;
- reafirmar decisões congeladas;
- informar estado atual do repositório;
- informar próxima decisão que depende do Owner.

## 10. Vigência

Este protocolo entra em vigor imediatamente e permanece ativo até revisão formal do Owner.
