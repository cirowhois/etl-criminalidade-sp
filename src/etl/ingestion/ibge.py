import os
import logging
from ftplib import FTP

logging.basicConfig(level=logging.INFO)

class IbgeIngestion:

    def __init__(self,json_config) -> None:
        self.__json_config = json_config

    def load_config(self,json):
        name = json['name']
        ftp_host = json['source']['ftp_host']
        ftp_url = json['source']['ftp_url']
        path = json['datalake_zones']['raw']['path']
        return name, ftp_host, ftp_url, path

    def download(self, ftp_host, ftp_url, path)->None:
        try:
            #ACCESS FTP
            ftp = FTP(ftp_host)
            ftp.login()
            ftp.cwd(ftp_url)
            #CREATE TARGET PATH
            os.makedirs(path, exist_ok=True)
            #DOWNLOAD FILES
            for file in ftp.nlst():
                logging.info(f"Dowloading IBGE data for file {file}")
                target_path = os.path.join(path, file)
                with open(target_path, 'wb') as f:
                    ftp.retrbinary(f"RETR {file}", f.write)
        except Exception as error:
            logging.error("Error to access IBGE files from FTP folder ",error)
            raise Exception("Error to access IBGE files from FTP folder ",error)
        
    def download_all(self)->None:
        name, ftp_host, ftp_url, path = self.load_config(self.__json_config)
        logging.info(f"Downloading {name}")
        self.download(ftp_host, ftp_url, path)
