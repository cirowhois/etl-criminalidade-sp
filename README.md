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
```
### Estrutura:
- src
    - data
        - raw 

            _Dado original conforme presente na fonte (formato, colunas, estrutura no geral)_
        - dispatch 

            _Dado com a estrutura original, mas salvo em .geoparquet, com alguns filtros e definição da coluna geom_
        - processed 

            _Dado com a estrutura e tratamentos finais, índices e tudo mais proposto no projeto para abrir com o kepler.gl_
    - etl
        - ingestion
            - `ibge.py`
            - `sspsp.py`
        - transformation _(em construção)_
    - `config.json` 

        _Configurações do projeto_
    - `utils.py` 

        _Pequena lib de apoio_
    - `ingestion.py` _(em construção)_

        _Apoio para ingestão/download dos dados de base_
- `requirements.txt`
- `setupe.sh`

    _Configura o ambiente com as bibliotecas necessárias_
