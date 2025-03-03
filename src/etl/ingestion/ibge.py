import os
import logging
import json
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
        name = json['name']
        ftp_host = json['ftp_host']
        ftp_url = json['ftp_url']
        source_format = json['format']
        file_name = json['file_name']
        path = json['raw_path']
        return name, ftp_host, ftp_url, source_format, file_name, path
    
    def unzip(self,path)->None:
        try:
            file = [file for file in os.listdir(path) if file.endswith(".zip")][0]
            file_path = os.path.join(path, file)
            logging.info(f"Unzipping {file}")
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(path)
            logging.info(f"{file} unzipped")
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
            logging.info(f"Downloading IBGE data for file {file_name}")
            target_path = os.path.join(path, file_name)
            with open(target_path, 'wb') as f:
                ftp.retrbinary(f"RETR {file_name}", f.write)
            logging.info(f"Data {file_name} downloaded")
        except Exception as e:
            logging.exception("Error to access IBGE file from FTP folder", exc_info=True)
            raise 
        
    def download_all(self)->None:
        config = self.load_json(self.__file_path)
        ibge_config = config["landing_area"]['ibge']
        for dict in ibge_config:
            name, ftp_host, ftp_url, source_format, file_name, path = self.load_config(dict)
            self.download(ftp_host, ftp_url, file_name, path)
            if source_format != 'zip':
                continue
            self.unzip(path)