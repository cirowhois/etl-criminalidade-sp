import os
import json
import zipfile
from ftplib import FTP
from utils import log_time, log_subsection

class IbgeIngestion:

    def __init__(self, config_path, logger):
        self.__file_path = config_path
        self.logger = logger

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
    
    @log_time
    def unzip(self,path)->None:
        file = [file for file in os.listdir(path) if file.endswith(".zip")][0]
        file_path = os.path.join(path, file)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(path)

    @log_time
    def download(self, ftp_host, ftp_url, file_name, path)->None:
        log_subsection(f" IBGE: {file_name}", self.logger)
        if not ftp_url.endswith('/'):
            ftp_url += '/'
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

    @log_time
    def download_all(self)->None:
        log_subsection(" IBGE - DOWNLOAD", self.logger)
        config = self.load_json(self.__file_path)
        ibge_config = config["landing_area"]['ibge']
        for dict in ibge_config:
            ftp_host, ftp_url, source_format, file_name, path = self.load_config(dict)
            self.download(ftp_host, ftp_url, file_name, path)
            if source_format != 'zip':
                continue
            self.unzip(path)
