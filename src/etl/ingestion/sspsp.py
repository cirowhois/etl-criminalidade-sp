import requests
import os
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils import log_time, log_subsection



class SSPSPIngestion:
    def __init__(self, config_path, logger):
        self.__file_path = config_path
        self.logger = logger

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)

    def load_config(self,json):
        years = json['years']
        ref_url = json['url']
        path = json['raw_path']
        return years, ref_url, path
    
    @log_time
    def download(self,url,path)->None:
        file_name = url.split('/')[-1]
        log_subsection(f" SSPSP: {file_name}", self.logger)
        os.makedirs(path, exist_ok=True)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Referer": "https://www.ssp.sp.gov.br/"
            }
        session = requests.Session()
        session.headers.update(headers)
        retries = Retry(total=26, backoff_factor=10, 
                        status_forcelist=[500, 502, 503, 504],
                        allowed_methods=["HEAD", "GET", "OPTIONS"])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        response = session.get(url,stream=True)
        if response.status_code == 200:
            with open(f"{path}/{file_name}", 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        else:
            raise

    @log_time
    def download_all(self):
        log_subsection(" SSPSP - DOWNLOAD", self.logger)
        config = self.load_json(self.__file_path)
        sspsp_config = config["landing_area"]['sspsp']
        for dict in sspsp_config:
            years, ref_url, path = self.load_config(dict)
            for year in years:
                self.download(ref_url.format(year),path)