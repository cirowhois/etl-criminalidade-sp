from etl.ingestion.ibge import IbgeIngestion
from etl.ingestion.sspsp import SSPSPIngestion, SSPSPcsv
from utils import JsonReader

config = JsonReader("config.json").read()
ibge_config = config['ibge']
sspsp_config = config['sspsp']

IbgeIngestion(ibge_config).download_all()
SSPSPIngestion(sspsp_config).download_all()
#SSPSPcsv(sspsp_config).write_csv()

