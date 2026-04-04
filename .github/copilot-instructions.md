# Projeto ETL Criminalidade SP

Pipeline ETL geoespacial para processar dados criminais do Estado de São Paulo (2017-2024), integrando ocorrências com geometrias territoriais oficiais (setores censitários e municípios) e dados sociodemográficos do Censo 2022.

## Stack

- **PySpark 3.5.3** + **Apache Sedona 1.7.0** — processamento distribuído e operações geoespaciais
- **GeoPandas 1.0.1** — operações geográficas locais
- **Kepler.GL 0.3.2** — visualização interativa de mapas
- **Unidecode** — normalização de texto

## Arquitetura Medallion

| Camada | Propósito | Exemplo de tabela |
|--------|-----------|-------------------|
| **Bronze** | Padronização: remove duplicatas/nulos, gera IDs SHA256, adiciona `YEAR_INFO` | `bronze_crimes_cellphones` |
| **Silver** | Limpeza + enriquecimento geoespacial: normaliza texto, corrige coordenadas, spatial joins | `silver_crimes`, `silver_scs_mun` |
| **Gold** | Agregação + índices: pivot de tipos de crime, cálculo de índices por mil/cem mil habitantes | `gold_crimes_scs`, `gold_crimes_mun` |

## Convenções de Código

- **Colunas identificadoras**: `ID_SC` (setor), `ID_MUN` (município), `ID_CRIME` (ocorrência) — hashes SHA256
- **Colunas temporais**: `YEAR_INFO`, `MONTH_INFO`, `DAY_INFO`, `DATE_PROCESSED`
- **Colunas tratadas**: sufixo `*_TREATED` (ex: `CRIMINAL_TYPE_TREATED`)
- **Geometrias**: coluna `geom`, sempre EPSG:4326
- **Particionamento**: por `YEAR_INFO` ou `ID_MUN`
- **Parquet**: máximo 1M registros por arquivo

## Padrão de Classes ETL

Todas as classes de step seguem o padrão:

```
read_data() → treat_data() → write_data() → test_data()
```

Referências: `BronzeStep` em `src/etl/steps/bronze.py`, `SilverIbgeStep`/`SilverSspspStep` em `silver.py`, `GoldStep` em `gold.py`.

## Configuração

- **Hub central**: `src/config.json` — configuração Spark, landing areas, regras de tabelas, testes de qualidade
- **Sessão Spark**: usar `get_sedona()` de `src/utils.py`
- **Logger**: usar `setup_logger()` de `src/utils.py`
- **Testes de qualidade**: config-driven via `test_unique()` e `test_null()` — definidos no `config.json` por tabela

## Diretrizes de Refatoração (Portabilidade + Orquestração)

- Priorizar desenho **cloud-agnóstico**: local, container e nuvem devem usar os mesmos parâmetros de entrada sempre que possível
- Preservar **Apache Sedona** como engine geoespacial principal em todas as camadas
- Para orquestração, adotar **Apache Airflow** com tarefas parametrizadas por camada/tabela/ano
- Garantir **idempotência por partição** (ex.: `YEAR_INFO`) e evitar `append` cego sem estratégia de deduplicação
- Reforçar execução incremental para novos anos (2025+) e capacidade de reprocessamento por ano sem efeito colateral
- Tratar leitura de Excel volumoso com estratégia de normalização intermediária antes do processamento distribuído

## Contrato Entre Agentes

- `@pipeline` é responsável por planejar e delegar; não implementa código nem documentação diretamente
- `@dev` implementa e valida mudanças técnicas; não cria changelog
- `@scriber` documenta mudanças já implementadas, sempre em PT-BR e no padrão Keep a Changelog
- Handoff mínimo de implementação para documentação deve conter:
	- Lista de arquivos alterados/criados
	- Resumo objetivo do que mudou
	- Camada(s) afetada(s): Bronze, Silver, Gold, Ingestão, Infra
	- Como validar a mudança (comando/teste/smoke)

## Critérios de Pronto (DoD)

- Mudança compatível com execução local e preparada para execução em ambiente orquestrado
- Regras de qualidade e partição mantidas ou evoluídas explicitamente
- Changelog criado em `changelog/` quando houver implementação concluída
- Sem regressão nas operações geoespaciais principais (`ST_Intersects`, `ST_Union_Aggr`, `ST_Transform`, `ST_Area`)

## Fontes de Dados

- **SSPSP** (Secretaria de Segurança Pública): ocorrências de celulares, veículos e outros crimes (Excel)
- **IBGE**: geometrias de setores censitários (GeoPackage) e dados sociodemográficos (CSV/zip)

## Documentação

- Idioma: **PT-BR**
- Changelog: pasta `changelog/` com um arquivo `.md` por implementação (formato Keep a Changelog)

### Convenção de Changelog

Nome do arquivo: `YYYY-MM-DD_descricao-breve.md`

Formato:

```markdown
# Título da Mudança

**Data**: YYYY-MM-DD
**Camada(s)**: Bronze | Silver | Gold | Ingestão | Infra

## Adicionado
- ...

## Modificado
- ...

## Corrigido
- ...

## Removido
- ...
```

Categorias seguem o padrão [Keep a Changelog](https://keepachangelog.com/pt-BR/). Incluir apenas as categorias que se aplicam.
