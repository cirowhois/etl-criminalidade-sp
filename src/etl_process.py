from etl.steps.bronze import BronzeStep
from etl.steps.silver import SilverIbgeStep, SilverSspspStep
from etl.steps.gold import GoldStep
from utils import get_sedona, setup_logger, log_section
import logging
import warnings

warnings.filterwarnings('ignore')
logger = setup_logger("ETL", level=logging.DEBUG)

# LOAD CONFIG
config_path = 'config.json'
sedona = get_sedona(config_path)

# LOAD ETL CLASSES
bronze = BronzeStep(config_path,sedona,logger)
silver_ibge = SilverIbgeStep(sedona,logger)
silver_sspsp = SilverSspspStep(sedona,logger)
gold = GoldStep(sedona,logger)

def main():
    log_section(" STARTING ETL PROCESS ", logger)

    # BRONZE
    log_section(" BRONZE ", logger)
    bronze.tables()

    # SILVER
    log_section(" SILVER ", logger)
    silver_ibge.tables()
    silver_sspsp.tables()

    #GOLD
    log_section(" GOLD ", logger)
    gold.tables()

    sedona.stop()
    log_section(" ETL PROCESS DONE ", logger)

if __name__ == "__main__":
    main()