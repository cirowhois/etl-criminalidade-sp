from etl.steps.bronze import AllBronze
from etl.steps.silver import IBGESilver, SSPSPSilver
from etl.steps.gold import AllGold
from utils import sedona
import logging
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

# LOAD CONFIG
config_path = 'config.json'

# LOAD ETL CLASSES
bronze_processor = AllBronze(config_path,sedona)
silver_ibge_processor = IBGESilver(sedona)
silver_sspsp_processor = SSPSPSilver(sedona)
gold_processor = AllGold(sedona)


logging.info("################# STARTING ETL PROCESS #################")

logging.info("######################## BRONZE ########################")
# BRONZE
bronze_processor.all_tables()

logging.info("######################## SILVER ########################")
# SILVER
silver_ibge_processor.silver_scs_demographics()
silver_ibge_processor.silver_mun_demographics()
silver_ibge_processor.silver_scs_geom()
silver_ibge_processor.silver_mun_geom()
silver_ibge_processor.silver_scs()
silver_ibge_processor.silver_mun()
silver_sspsp_processor.silver_crimes_cellphones()
silver_sspsp_processor.silver_crimes_vehicles()
silver_sspsp_processor.silver_crimes_others()
silver_sspsp_processor.silver_crimes()

logging.info("######################## GOLD ########################")
# GOLD 
gold_processor.gold_crimes()
gold_processor.gold_crimes_scs()
gold_processor.gold_crimes_mun()

logging.info("################# FINISHED ETL PROCESS #################")
sedona.stop()