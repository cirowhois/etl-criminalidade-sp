from etl.ingestion.ibge import IbgeIngestion
from etl.ingestion.sspsp import SSPSPIngestion
from utils import setup_logger, log_section
import logging
import warnings

warnings.filterwarnings('ignore')
logger = setup_logger("INGESTION", level=logging.DEBUG)

# LOAD CONFIG
config_path = 'config.json'

# LOAD ETL CLASSES
ibge = IbgeIngestion(config_path, logger)
sspsp = SSPSPIngestion(config_path, logger)

def main():
    log_section(" STARTING INGESTION PROCESS ", logger)

    ibge.download_all()
    sspsp.download_all()

    log_section(" INGESTION PROCESS DONE ", logger)

if __name__ == "__main__":
    main()