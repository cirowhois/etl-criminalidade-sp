import os
import logging
import json
import time
import zipfile
from ftplib import FTP

logging.basicConfig(level=logging.INFO)

class IbgeIngestion:

    def __init__(self, config_path):
        self.__file_path = config_path

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)

    def load_config(self,json):
        ftp_host = json['ftp_host']
        ftp_url = json['ftp_url']
        source_format = json['format']
        file_name = json['file_name']
        path = json['raw_path']
        return ftp_host, ftp_url, source_format, file_name, path
    
    def unzip(self,path)->None:
        try:
            file = [file for file in os.listdir(path) if file.endswith(".zip")][0]
            file_path = os.path.join(path, file)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(path)
        except Exception as e:
            logging.exception("Error to unzip IBGE file", exc_info=True)
            raise 

    def download(self, ftp_host, ftp_url, file_name, path)->None:
        if not ftp_url.endswith('/'):
            ftp_url += '/'
        try:
            # ACCESS FTP
            ftp = FTP(ftp_host)
            ftp.login()
            ftp.cwd('/'.join(ftp_url.split('/')[:-1]))
            # CREATE TARGET PATH
            os.makedirs(path, exist_ok=True)
            # DOWNLOAD SPECIFIC FILE
            target_path = os.path.join(path, file_name)
            with open(target_path, 'wb') as f:
                ftp.retrbinary(f"RETR {file_name}", f.write)
        except Exception as e:
            logging.exception("Error to access IBGE file from FTP folder", exc_info=True)
            raise 
        
    def download_all(self)->None:
        logging.info("###### INGESTION - IBGE")
        config = self.load_json(self.__file_path)
        ibge_config = config["landing_area"]['ibge']
        for dict in ibge_config:
            start_time = time.time()
            ftp_host, ftp_url, source_format, file_name, path = self.load_config(dict)
            logging.info(f"###### INGESTION - IBGE - {file_name}")

            self.download(ftp_host, ftp_url, file_name, path)
            if source_format != 'zip':
                continue
            self.unzip(path)
            
            elapsed = time.time() - start_time
            logging.info(f"###### INGESTION - IBGE - {file_name} - DONE IN {elapsed:.2f}s")
