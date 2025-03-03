import requests
import logging
import os
import pandas as pd
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)


class SSPSPIngestion:
    def __init__(self, config_path):
        self.__file_path = config_path

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)

    def load_config(self,json):
        name = json['name']
        years = json['years']
        ref_url = json['url']
        path = json['raw_path']
        return name, years, ref_url, path
       
    def download(self,url,path)->None:
        try:
            file_name = url.split('/')[-1]
            logging.info(f"Dowloading criminal data for {file_name}")
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
                logging.info(f"Data {file_name} downloaded")
            else:
                logging.error(f"Error to download data {response.status_code}")
                raise
        except Exception as e:
            logging.exception("Error to download data", exc_info=True)
            raise

    def download_all(self):
        config = self.load_json(self.__file_path)
        sspsp_config = config["landing_area"]['sspsp']
        for dict in sspsp_config:
            name, years, ref_url, path = self.load_config(dict)
            logging.info(f"Downloading SSPSP data for {name}")
            for year in years:
                #logging.info(ref_url.format(year))
                self.download(ref_url.format(year),path)
