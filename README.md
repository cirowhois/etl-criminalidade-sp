# Projeto Aplicado XPE - Criminalidade SP

## Apresentação
O projeto propõe a criação de um pipeline robusto para processar e integrar dados de criminalidade do Estado de São Paulo, utilizando informações geográficas de alta granularidade. O objetivo principal é extrair, transformar e carregar grandes volumes de dados históricos (2017-2024) para servir de insumo de análises geográficas de eventos criminais.

A abordagem concentra-se no processamento eficiente dos dados geográficos, integrando informações de ocorrências criminais com geometrias territoriais oficiais (setores censitários e municípios) e dados sociodemográficos. Isso possibilitará a criação de métricas como índices de ocorrência por x mil habitantes, facilitando a comparação entre diferentes áreas por mais detalhadas que sejam.

O projeto utilizou o [Apache Sedona - antigo Geospark -](https://sedona.apache.org/latest/) que funciona com base no Spark, uma vez que o Spark não tem suporte para trabalhar diretamente com geometrias. São dados volumosos de ocorrências criminais e de geometrias polígonais intraurbanas (Setores Censitários), o que exige ferramentas preparadas para lidar com dados volumosos que interagem entre si geograficamente. 

Para gerar os exemplos foi utilizado o [kepler.gl Jupyter](https://docs.kepler.gl/docs/keplergl-jupyter), onde a partir de um notebook e de um dataset, é possível gerar um html contendo um mapa interativo desse dataset.


## Fontes
- SSPSP: https://www.ssp.sp.gov.br/estatistica/consultas
    - Celulares (2017 - 2024);
    - Veículos (2017 - 2024);
    - Outros Crimes (2022 - 2024).     
- IBGE: ['Malha de Setores Censitários - Censo 2022'](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/gpkg/BR/)
- IBGE: ['Resultados por Setores Censitários - Censo 2022'](https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/)


## Docs

### Preparar o ambiente
- Clonar o projeto;
- Criar um ambiente virtual (chamado _venv_ mesmo) na pasta do projeto;
- Executar o `setup.sh`;
- Obs: É necessário ter o Python e o Spark instalados na máquina. Aqui foram utilizados `Spark 3.5.3/Scala 2.12` e o `Python 3.12.3`.

### ETL
- **Ingestão**: em _src_ executar o `ingestion.py`. Os dados serão salvos em _src/data/landing_area_ \
- **ETL**: em _src_ executar o `etl_process.py`. Os dados serão salvos em _src/data/bronze ou silver ou gold/tb_name=_ \
- **Logs**: Os logs serão gerados em _src/logs_, o `log_reader.py` gera um csv para os logs do ETL. 

### Resultados
Foram gerados três exemplos de mapas de acordo com os níveis geográficos para os dados tratados no projeto (Ocorrência, Setores Censitários e Municípios) em _html/kepler_:
- Ocorrências: [mapa_geral_campinas.html](https://github.com/cirowhois/etl-criminalidade-sp/blob/main/html/kepler/mapa_geral_campinas.html)
- Setores Censitários: [mapa_scs_rmc_2024.html](https://github.com/cirowhois/etl-criminalidade-sp/blob/main/html/kepler/mapa_scs_rmc_2024.html)
- Municípios: [mapa_mun_2024.html](https://github.com/cirowhois/etl-criminalidade-sp/blob/main/html/kepler/mapa_mun_2024.html)
    
Basta baixar o html e abrir no navegador, ele contém um mapa interativo - _um 'webgis alike'_ - que permite visualizar o conjunto de dados usado em cada exemplo. \
PAra gerar outros exemplos de outras áreas e outros períodos é só utilizar o `maps_examples.ipynb`.


## Arquitetura Geral e Lineage
![etl_ciro drawio (3)](https://github.com/user-attachments/assets/435c6bd4-b4f2-472f-a290-566da90dac4d)
![image](https://github.com/user-attachments/assets/7a97c77c-06c9-4cc5-874d-f1533dbdf16b)

## Licença
Este projeto é um projeto de conclusão de curso. O uso do script é livre, mas o uso e a divulgação dos dados podem estar sujeitos às regras de cada fonte, especialmente da Secretaria de Segurança Pública de São Paulo.

## Contato
[Linkedin - Ciro](https://www.linkedin.com/in/ciroruiz95/) \
[Instagram - CartoArte](https://www.instagram.com/cartoarte/)
