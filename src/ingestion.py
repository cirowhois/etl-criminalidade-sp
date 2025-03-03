from etl.ingestion.ibge import IbgeIngestion
from etl.ingestion.sspsp import SSPSPIngestion

IbgeIngestion('config.json').download_all()
SSPSPIngestion('config.json').download_all()
