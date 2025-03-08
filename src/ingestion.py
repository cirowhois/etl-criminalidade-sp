from etl.ingestion.ibge import IbgeIngestion
from etl.ingestion.sspsp import SSPSPIngestion
import logging

logging.basicConfig(level=logging.INFO)

logging.info("################# STARTING INGESTION PROCESS #################")

IbgeIngestion('config.json').download_all()
SSPSPIngestion('config.json').download_all()

logging.info("################# FINISHED INGESTION PROCESS #################")

