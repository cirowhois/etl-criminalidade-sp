import os
import logging
import fiona
import pyspark.sql.functions as f
import pyspark.pandas as ps
import pandas as pd
import time
import warnings
import json
from sedona.spark import *
from pyspark.sql.types import NullType


warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

class AllBronze:
    def __init__(self,config_path):
        self.__config_path = config_path

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)
        
    def load_config(self,json):
        logging.info("Reading config file")
        sedona_config = json['spark_config']
        bronze_config = json['data_lake']['bronze']
        logging.info("Config file read")
        return sedona_config, bronze_config
    
    def load_sedona_config(self,sedona_config):
        spark_master = sedona_config['spark_master']
        spark_driver_memory = sedona_config['spark_driver_memory']
        spark_executor_memory = sedona_config['spark_executor_memory']
        spark_executor_instances = sedona_config['spark_executor_instances']
        spark_driver_cores = sedona_config['spark_driver_cores']
        spark_executor_cores = sedona_config['spark_executor_cores']
        sedona_jars = sedona_config['sedona_jars']['packages']
        sedona_repo = sedona_config['sedona_jars']['repositories']
        return spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo
    
    def s_config(self, name, spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo) -> SedonaContext:
        try:
            return SedonaContext.builder().appName(name) \
                                .master(spark_master) \
                                .config("spark.driver.memory", spark_driver_memory) \
                                .config("spark.executor.memory", spark_executor_memory) \
                                .config("spark.executor.instances", spark_executor_instances) \
                                .config("spark.driver.cores", spark_driver_cores) \
                                .config("spark.executor.cores", spark_executor_cores) \
                                .config("spark.jars.packages", sedona_jars) \
                                .config("spark.jars.repositories", sedona_repo) \
                                .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
                                .getOrCreate()
        except Exception as e:
            logging.exception("Error creating SedonaContext", exc_info=True)
            raise

    def sedona_context(self,s_config):
        return SedonaContext.create(s_config)

    def list_sheets(self,file,year):
        return [sheet for sheet in pd.ExcelFile(file.format(year)).sheet_names if sheet not in (('DICIONÁRIO DE DADOS', 'METODOLOGIA'))]

    def gpkg_layer(self,file):
        return fiona.listlayers(file)[0]

    def read_data(self,sedona,file,year):
        try:
            stage_time = time.time()
            if file.endswith('.csv'):
                logging.info(f"Reading data {file.split('/')[-1]}")
                data = sedona.read.csv(file, sep=';', encoding='Windows-1252', header=True, inferSchema=False)
                elapsed_time = time.time() - stage_time
                logging.info(f"Data {file.split('/')[-1]} read in {elapsed_time}s")
                return data
            elif file.endswith('.gpkg'):
                logging.info(f"Reading data {file.split('/')[-1]}")
                layer = self.gpkg_layer(file)
                data = sedona.read.format("geopackage").option("tableName",layer).load(file)
                elapsed_time = time.time() - stage_time
                logging.info(f"Data {file.split('/')[-1]} read in {elapsed_time}s")
                return data
            elif file.endswith('.xlsx'):
                logging.info(f"Reading data {file.format(year).split('/')[-1]}")
                sheets = self.list_sheets(file,year)
                df_list = []
                for sheet in sheets:
                    df = ps.read_excel(file.format(year), sheet_name=sheet, dtype=str)
                    df_list.append(df)
                if len(df_list) > 1:
                    data = ps.concat(df_list, ignore_index=True)
                else:
                    data = df
                data = data.to_spark()
                elapsed_time = time.time() - stage_time
                logging.info(f"Data {file.format(year).split('/')[-1]} read in {elapsed_time}s")
                return data
            else:
                elapsed_time = time.time() - stage_time
                raise ValueError("Unsupported file format")
        except Exception as e:
            logging.exception("Error to access raw data", exc_info=True)
            raise

    def treat_columns(self, data):
        try:
            logging.info("Treating columns")
            stage_time = time.time()
            # REMOVE DUPLICATED COLUMNS
            valid_columns = [field.name for field in data.schema.fields if not field.name.endswith(tuple(f".{i}" for i in range(1, 10)))]
            data = data.select(valid_columns)
            # REMOVE NULL COLUMNS
            valid_columns = [field.name for field in data.schema.fields if not isinstance(field.dataType, NullType)]
            data = data.select(valid_columns)
            elapsed_time = time.time() - stage_time
            logging.info(f"Columns treated in {elapsed_time}s")
            return data
        except Exception as e:
            logging.exception("Error treating columns", exc_info=True)
            raise
    
    def treat_data(self,data,year):
        try:
            logging.info("Adding ref columns")
            stage_time = time.time()
            data = data.withColumn("DATE_PROCESSED",f.current_date()) \
                       .withColumn("YEAR_INFO", f.lit(year))
            elapsed_time = time.time() - stage_time
            logging.info(f"Columns added in {elapsed_time}s")
            return data
        except Exception as e:
            logging.exception("Error to add column", exc_info=True)
            raise
    
    def write_data(self,data, output_path,format)->None:
        try:
            logging.info(f"Writing data")
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO").option("maxRecordsPerFile",100000).mode('append').format(format).parquet(output_path)
            elapsed_time = time.time() - stage_time
            logging.info(f"Data written in {elapsed_time}s")
        except Exception as e:
            logging.exception("Error to write data", exc_info=True)
            raise

    def all_tables(self):
        logging.info("Generating bronze data")
        start_time = time.time()
        # SETTING UP CONFIGS
        config = self.load_json(self.__config_path)
        sedona_config, bronze_config = self.load_config(config)
        spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(sedona_config)
        output_prefix = 'data/bronze'
        for table in bronze_config:
            name = table['name']
            input = table['input']
            output = os.path.join(output_prefix,f"tb_name={table['output']}")
            format = table['format']
            years = table['years']
            # SET SEDONA
            sedona_config = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_config)
            # INPUT
            for year in years:
                data = self.read_data(sedona,input,year)
                # TRANSFORM
                data = self.treat_columns(data)
                data = self.treat_data(data,year)
                # WRITE
                self.write_data(data,output,format)
            sedona.stop()
        elapsed_time = start_time = time.time() - start_time
        logging.info(f"Bronze data processed in {elapsed_time}s")
        
