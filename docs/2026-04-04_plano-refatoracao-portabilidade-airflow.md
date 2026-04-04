# Plano de Refatoração: Portabilidade + Airflow (com Apache Sedona)

**Data**: 2026-04-04  
**Status**: Fase 1A concluída — Fase 1B pendente  
**Escopo**: Refatoração balanceada para execução local/container/nuvem (cloud-agnostic), mantendo Apache Sedona como engine geoespacial principal.

## Objetivo

Evoluir o ETL atual (script local) para um pipeline mais estruturado, com:

- Portabilidade de runtime
- Idempotência por partição
- Ingestão robusta de Excel volumoso
- Orquestração com Apache Airflow
- Governança de agentes para acelerar entregas sem retrabalho

## Decisões de Arquitetura

- Orquestrador alvo: **Apache Airflow**
- Estratégia de ambiente: **local-first + cloud-agnostic**
- Engine geoespacial: **Apache Sedona** (mantido)
- Estratégia de execução: **incremental por ano/partição**, com reprocessamento seguro

## Fase 0 (Concluída): Governança de Agentes e Instruções

### Entregas

- Alinhamento de diretrizes do projeto para:
  - portabilidade
  - idempotência por partição
  - Airflow como alvo de orquestração
  - preservação de operações geoespaciais críticas (Sedona)
- Clarificação de papéis:
  - `@pipeline` planeja/delega
  - `@dev` implementa/valida
  - `@scriber` documenta em PT-BR (Keep a Changelog)
- Contrato mínimo de handoff técnico antes de documentação

### Arquivos ajustados na Fase 0

- `.github/copilot-instructions.md`
- `.github/agents/dev.agent.md`
- `.github/agents/pipeline.agent.md`
- `.github/agents/scriber.agent.md`

## Fase 1: Baseline e Portabilidade (Fundação)

1. Criar camada de runtime/config para resolver:
   - `config_path`
   - `data_root`
   - `log_dir`
   - `spark_profile`
   via CLI + variáveis de ambiente.
2. Remover acoplamentos a CWD e paths hardcoded.
3. Evoluir inicialização Spark/Sedona para perfis (local/container/cluster).
4. Adicionar metadados mínimos de execução nas saídas (`RUN_ID`, `PROCESS_DATE`, `STAGE`, `TABLE`).

### Progresso — Fase 1A (concluída em 2026-04-04)

- ✅ `src/runtime.py` criado — módulo central de resolução de parâmetros (`config_path`, `data_root`, `log_dir`, `spark_profile`, `run_id`, `stage_filter`, `year_filter`) via kwarg > env > default
- ✅ `src/utils.py` atualizado — `get_sedona` aceita `spark_profile`; `setup_logger` aceita `log_dir`
- ✅ `src/etl_process.py` integrado com `get_runtime()`; inicialização Spark movida para lazy (dentro de `main()`)
- ✅ `src/ingestion.py` integrado com `get_runtime()`
- ✅ `src/etl/steps/bronze.py`, `silver.py`, `gold.py` — `data_root` parametrizado; todos os paths hardcoded removidos (16 em silver, 9 em gold, `output_prefix` em bronze)

**Validações executadas:**
- `py_compile` em 7 arquivos: ✅ PASS
- `from runtime import get_runtime; rt = get_runtime()`: ✅ PASS
- `from utils import get_sedona, setup_logger`: ✅ PASS
- AST parse de construtores com `data_root` em todas as steps: ✅ PASS
- Grep zero paths hardcoded: ✅ PASS
- Nota: `import etl_process` bloqueado por incompatibilidade pré-existente `pyspark.pandas` vs `pandas` 3.x (não introduzida nesta fase)

**Próxima fase:** Fase 1B — Contrato de escrita e idempotência por partição

## Fase 2: Ingestão Robusta e Idempotência

1. Adotar fluxo em duas etapas para Excel:
   - landing raw
   - normalização intermediária (ex.: Parquet tipado)
2. Trocar `append` cego por estratégia determinística por partição-alvo.
3. Permitir reprocessamento por ano sem duplicidade.
4. Padronizar contratos de qualidade por camada:
   - unique
   - not_null
   - volume/completude temporal

## Fase 3: Orquestração com Airflow

1. Expor tarefas parametrizadas por camada/tabela/ano:
   - ingest
   - bronze
   - silver
   - gold
   - quality
2. Implementar DAG com:
   - dependências explícitas
   - retries
   - SLA básico
   - backfill por ano
3. Adicionar trilha de metadados de execução:
   - status
   - row_count
   - duração
   - erro
   - hash de entrada

## Fase 4: Empacotamento e Operação Multiambiente

1. Containerizar runtime (Python + Java + Spark + Sedona).
2. Garantir execução local e em container com os mesmos parâmetros.
3. Criar CI mínima:
   - lint
   - testes
   - smoke ETL com amostra
4. Consolidar runbooks de operação (local, docker, airflow local, cloud).

## Verificação de Pronto por Fase

1. Execução local de referência preserva resultado lógico das tabelas.
2. Rerun do mesmo ano não gera duplicidade.
3. Operações Sedona críticas sem regressão:
   - `ST_Intersects`
   - `ST_Union_Aggr`
   - `ST_Transform`
   - `ST_Area`
4. DAG Airflow local funciona com falha simulada + retry.
5. Smoke em container roda com paths externos.

## Ordem Recomendada para Implementação

1. Fase 1 (portabilidade)
2. Fase 2 (idempotência + ingestão Excel)
3. Fase 3 (Airflow)
4. Fase 4 (container + CI)

## Handoff Padrão entre Agentes

Ao passar implementação para documentação, incluir sempre:

- arquivos alterados/criados
- resumo objetivo do que mudou
- camada(s) afetada(s): Bronze, Silver, Gold, Ingestão, Infra
- comandos/testes/smoke executados e resultado resumido

## Nota de Uso em Novas Sessões

Para aproveitar melhor a janela de contexto, inicie a sessão pedindo para o agente:

1. Ler este plano completo.
2. Confirmar status da fase atual.
3. Executar apenas a próxima fase priorizada.
4. Atualizar este arquivo ao final com progresso real (checklist + evidências).
