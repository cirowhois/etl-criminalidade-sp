## Plan: Refatoracao Detalhada ETL Sedona + Airflow

Detalhar a execucao da refatoracao por fases, amarrando cada tarefa aos arquivos e simbolos existentes do projeto para reduzir ambiguidade durante a implementacao. A estrategia preserva o comportamento atual de Bronze/Silver/Gold, corrige acoplamentos locais e prepara o pipeline para orquestracao com Airflow sem perder compatibilidade local.

**Steps**
1. Fase 1A - Runtime unico e desacoplamento de ambiente (fundacao)
2. Criar um modulo de runtime (ex.: src/runtime.py) para resolver parametros de execucao: config_path, data_root, log_dir, run_id, spark_profile, stage_filter, year_filter. O modulo deve ler CLI > variaveis de ambiente > defaults. *bloqueia 1B-1D* ✅ CONCLUÍDO
3. Atualizar src/etl_process.py e src/ingestion.py para usar runtime central e remover config_path hardcoded. Preservar modo full-run atual como default. *depende de 2* ✅ CONCLUÍDO
4. Atualizar src/utils.py em setup_logger() e get_sedona() para usar runtime/spark_profile (local/container/cluster), sem quebrar local[*] por default. *depende de 2* ✅ CONCLUÍDO
5. Trocar paths literais de data em classes de step para paths derivados de data_root (Bronze/Silver/Gold/landing). Priorizar BronzeStep.tables(), SilverIbgeStep.*, SilverSspspStep.*, GoldStep.*. *depende de 2; paralelo com 4 se contrato de runtime fechado* ✅ CONCLUÍDO

6. Fase 1B - Contrato de escrita e idempotencia por particao
7. Definir politica de escrita por camada:
   - Bronze: overwrite por particao YEAR_INFO (ou replaceWhere equivalente)
   - Silver: overwrite por particao-alvo mantendo particionamento existente
   - Gold: overwrite por particao-alvo mantendo agregacoes
   Incluir regra de rerun para um ano sem apagar anos nao alvo. *depende de 5*
8. Ajustar BronzeStep.write_data() (hoje append) para escrita deterministica por ano; validar impacto em BronzeStep.test_data(). *depende de 7*
9. Propagar run metadata (RUN_ID, PROCESS_DATE, STAGE, TABLE) em outputs principais sem alterar chaves de negocio. *depende de 7; paralelo com 8*

10. Fase 1C - Ingestao robusta para excel volumoso
11. No fluxo SSPSP (src/etl/ingestion/sspsp.py + BronzeStep.read_data()), separar:
   - download raw xlsx
   - normalizacao intermediaria por ano para formato eficiente (parquet tipado)
   Bronze passa a priorizar leitura do intermediario e manter xlsx como fallback. *depende de 5*
12. Implementar estrategia de selecao de download na ingestao com 3 modos explicitamente suportados:
   - missing_only: baixa apenas o que nao existe localmente (ou esta invalido)
   - years_list: baixa apenas anos solicitados (ex.: 2023,2025)
   - force: rebaixa anos especificados mesmo que ja existam
   A selecao deve aceitar filtro por dataset (cellphones|vehicles|others) e por ano. *depende de 11*
13. Adicionar manifesto local de ingestao (ex.: data/metadata/ingestion_manifest.parquet ou json) com: dataset, year, source_url, file_name, etag/last_modified quando disponivel, file_size, checksum opcional, downloaded_at, status. Usar esse manifesto para decidir download incremental. *depende de 12*
14. Adicionar descoberta de anos novos para SSPSP com modo controlado (auto_discover=true/false) e persistencia de anos processados em metadata de execucao. *depende de 13*
15. Reforcar resiliencia de download IBGE/SSPSP (retry + timeout + validacao de integridade + skip se arquivo ja valido). *paralelo com 14*

14. Fase 1D - Qualidade e observabilidade base
15. Evoluir run_tests() e testes config-driven para retornar resultado estruturado por tabela (status, contagens, regras avaliadas) alem de falhar quando exigido. *depende de 8*
16. Criar trilha minima de metadados de execucao por etapa (run_id, stage, table, year, start/end, row_count, error_msg, validation_status). Pode iniciar em parquet local no data_root/metadata. *depende de 9 e 15*

17. Fase 2 - Interface para orquestracao Airflow
18. Extrair wrappers de tarefa por camada/tabela/ano (ex.: src/tasks/*.py) reaproveitando classes atuais:
   - ingest_task
   - bronze_task
   - silver_ibge_task
   - silver_sspsp_task
   - gold_task
   - quality_task
   Cada task deve receber parametros explicitos e retornar resumo de execucao. *depende de 16*
19. Reestruturar src/etl_process.py para orquestrar via tasks com possibilidade de filtro por camada/tabela/ano. *depende de 18*

20. Fase 3 - DAG Airflow local-first
21. Criar DAG (ex.: dags/etl_criminalidade_sp.py) com dependencias:
   ingestao -> bronze -> silver_ibge + silver_sspsp -> gold -> quality -> publish_metadata
   Incluir retries, timeout por task, backfill por ano, e parametros por run. *depende de 19*
22. Configurar execucao local com docker-compose Airflow e volumes para data/logs/config. *depende de 21; paralelo com 23*
23. Preparar perfil cloud-agnostico (storage endpoint e spark master externos via env) sem acoplamento a provider especifico. *depende de 4 e 21*

24. Fase 4 - Operacao, CI minima e runbooks
25. Adicionar pipeline CI basico:
   - lint/style
   - checagem de import/runtime
   - smoke ETL reduzido (amostra)
   - validacao de DAG parse
   *depende de 21*
26. Atualizar documentacao operacional com comandos por ambiente (local puro, local docker, airflow local, cloud-agnostic). *depende de 22 e 25*
27. Definir rotina de evolucao anual (2025+) com checklist de ingestao, qualidade e backfill. *depende de 26*

**Relevant files**
- /home/cirowhois/code/etl-criminalidade-sp/src/etl_process.py - orquestrador atual full-run; ponto de entrada para runtime e filtros
- /home/cirowhois/code/etl-criminalidade-sp/src/ingestion.py - entrypoint de download; deve receber parametros de runtime
- /home/cirowhois/code/etl-criminalidade-sp/src/utils.py - get_sedona(), setup_logger(), run_tests(); principal acoplamento local
- /home/cirowhois/code/etl-criminalidade-sp/src/config.json - spark_config, landing_area, data_lake e testes por tabela
- /home/cirowhois/code/etl-criminalidade-sp/src/etl/steps/bronze.py - read_data() com xlsx via pyspark.pandas e write_data() append atual
- /home/cirowhois/code/etl-criminalidade-sp/src/etl/steps/silver.py - SilverIbgeStep e SilverSspspStep com paths literais e ST_Intersects
- /home/cirowhois/code/etl-criminalidade-sp/src/etl/steps/gold.py - agregacoes e indices; ajustar metadata e escrita por particao
- /home/cirowhois/code/etl-criminalidade-sp/src/etl/ingestion/sspsp.py - download HTTP com retries; evoluir para descoberta anual e validacao de arquivo
- /home/cirowhois/code/etl-criminalidade-sp/src/etl/ingestion/ibge.py - download FTP + unzip; robustez e idempotencia de ingestao
- /home/cirowhois/code/etl-criminalidade-sp/logs/log_reader.py - referencia para metrica operacional inicial
- /home/cirowhois/code/etl-criminalidade-sp/.github/copilot-instructions.md - regras macro da refatoracao e contrato entre agentes
- /home/cirowhois/code/etl-criminalidade-sp/.github/agents/dev.agent.md - implementacao e validacao tecnica
- /home/cirowhois/code/etl-criminalidade-sp/.github/agents/pipeline.agent.md - orquestracao do fluxo plan->dev->scriber
- /home/cirowhois/code/etl-criminalidade-sp/.github/agents/scriber.agent.md - documentacao em PT-BR e changelog por implementacao

**Verification**
1. Baseline local: executar ingestao + etl full atual e registrar contagens/chaves por tabela de referencia (ao menos bronze_crimes_cellphones, silver_crimes, gold_crimes_scs, gold_crimes_mun).
2. Portabilidade: executar com paths e config via env em diretorio diferente do repo e confirmar que logs/data sao gerados corretamente.
3. Idempotencia: reprocessar mesmo ano duas vezes e confirmar ausencia de duplicidades nas chaves unicas configuradas.
4. Sedona parity: comparar cardinalidade e area agregada para amostra de municipios apos mudancas em SilverIbgeStep e SilverSspspStep.
5. Ingestao excel: medir tempo/memoria do fluxo xlsx direto vs fluxo intermediario normalizado em ao menos um ano de cellphones e vehicles.
6. Qualidade estruturada: validar que cada tabela retorna status de regras unique/not_null e falha controlada quando regra quebra.
7. Airflow DAG: validar parse, dry-run, uma execucao de backfill de 1 ano e comportamento de retry em falha simulada.

**Decisions**
- Incluir no escopo imediato apenas refatoracao estrutural necessaria para portabilidade + Airflow; sem migracao para lakehouse especifico.
- Manter convencoes existentes de nomes e geometria (geom EPSG:4326) para evitar quebra analitica.
- Introduzir novos modulos (runtime/tasks/metadata) de forma aditiva para minimizar impacto nas classes atuais.
- Preservar comportamento default de execucao full para compatibilidade retroativa.

**Further Considerations**
1. GoldStep.treat_data_mun() usa hash inconsistente (f.sha) em ID_MUN_CRIME enquanto outros pontos usam sha2; padronizar durante Fase 1B para evitar divergencia de chave.
2. SilverIbgeStep.treat_data_scs_geom() aplica ST_Transform em geom ja transformada no fluxo; revisar sequencia para evitar transformacao redundante e risco numerico.
3. Definir politica de volume de arquivo por tabela no config.json (hoje maxRecordsPerFile esta hardcoded por metodo/chamada) para tuning por ambiente.
---

**Progresso**

- **Data de execução:** 2026-04-04
- **Fase concluída:** Fase 1A — Runtime único e desacoplamento de ambiente

**Validações executadas:**
- `py_compile` em 7 arquivos: ✅ PASS
- `from runtime import get_runtime; rt = get_runtime()`: ✅ PASS
- `from utils import get_sedona, setup_logger`: ✅ PASS
- AST parse de construtores com `data_root` em todas as steps: ✅ PASS
- Grep zero paths hardcoded: ✅ PASS
- Nota: `import etl_process` bloqueado por incompatibilidade pré-existente `pyspark.pandas` vs `pandas` 3.x (não introduzida nesta fase)

**Próxima fase:** Fase 1B — Contrato de escrita e idempotência por partição (steps 7–9)
