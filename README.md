# Projeto Aplicado XPE

### Fontes:
- SSPSP: https://www.ssp.sp.gov.br/estatistica/consultas 
- IBGE: ['Malha de Setores Censitários - Censo 2022'](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/gpkg/BR/)

### Ambiente:
```
- Base
    - Spark 3.5.3/Scala 2.12
    - Python 3.12.3
- Requirements
    - apache-sedona==1.7.0
    - fiona==1.10.1
    - folium==0.19.2
    - geopandas==1.0.1
    - keplergl==0.3.2
    - matplotlib==3.10.0
    - npm==0.1.1
    - numpy==1.26.4
    - pandas==1.5.3
    - pyproj==3.7.0
    - pyspark==3.5.3
    - tqdm==4.67.1
    - wget==3.2
    - openpyxl==3.1.5
    - pyarrow==19.0.1
    - pydeck==0.9.1
    - keplergl==0.3.2
    - unidecode==1.3.8
```
### Estrutura:
- src
    - data
        - landing_area: _Dado original conforme presente na fonte (formato, colunas, estrutura no geral)_
        - bronze: _Dado com a estrutura original, em .parquet, com campos de versão_
        - silver: _Dado em tratamento, com 'geometry field' e deduplicado_
        - gold: _Dado com a estrutura e tratamentos finais, índices e tudo mais proposto no projeto para abrir com o pydeck_
    - etl
        - ingestion
            - `ibge.py`
            - `sspsp.py`
        - steps
            - `bronze.py`
            - `silver.py`
            - `gold.py`
        - logs
    - `config.json`
    - `ingestion.py`: _Ingestão/download dos dados de base_
    - `etl_process.py`: _ETL do projeto_
- html/kepler _exemplos de uso do dado_
    - mapa_geral_campinas.html
    - mapa_scs_rmc_2024.html
    - mapa_mun_2024.html
- venv: _Ambiente virtual do projeto_
- `requirements.txt`
- `setup.sh`: _Configura o ambiente com as bibliotecas necessárias_

