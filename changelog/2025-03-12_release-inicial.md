# Release Inicial

**Data**: 2025-03-12  
**Camada(s)**: Bronze, Silver, Gold, Ingestão, Infra

## Adicionado
- Pipeline ETL completo com arquitetura medallion (Bronze → Silver → Gold)
- Camada Bronze: padronização de dados SSPSP (celulares, veículos, outros crimes) e IBGE (geometrias, sociodemográficos)
- Camada Silver: limpeza de texto, correção de coordenadas, spatial joins com setores censitários, transformação de geometrias IBGE
- Camada Gold: agregação por setor censitário e município com pivot de tipos de crime e cálculo de índices por mil/cem mil habitantes
- Ingestão automatizada de dados SSPSP (HTTP) e IBGE (FTP)
- Configuração centralizada via `src/config.json`
- Testes de qualidade config-driven (`test_unique`, `test_null`)
- Visualização interativa com Kepler.GL (mapas de municípios, setores censitários e ocorrências)
- Sistema de logging com `setup_logger()` e decorator `@log_time`
- Monitoramento de performance via `log_reader.py`
- Notebook de avaliação comparando GeoPandas vs Sedona
