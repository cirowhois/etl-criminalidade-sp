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
    def __init__(self,config_path,sedona):
        self.sedona = sedona
        config = self.load_json(config_path)
        self.bronze_config = self.load_config(config)

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)
        
    def load_config(self,json):
        bronze_config = json['data_lake']['bronze']
        return bronze_config
    
    def list_sheets(self,file,year):
        return [sheet for sheet in pd.ExcelFile(file.format(year)).sheet_names if sheet not in (('DICIONÁRIO DE DADOS', 'METODOLOGIA'))]

    def gpkg_layer(self,file):
        return fiona.listlayers(file)[0]

    def read_data(self, file, year):
        try:
            if file.endswith('.csv'):
                data = self.sedona.read.csv(file, sep=';', encoding='Windows-1252', header=True, inferSchema=False)
            elif file.endswith('.gpkg'):
                layer = self.gpkg_layer(file)
                data = self.sedona.read.format("geopackage").option("tableName", layer).load(file)
            elif file.endswith('.xlsx'):
                sheets = self.list_sheets(file, year)
                df_list = []
                for sheet in sheets:
                    df = ps.read_excel(file.format(year), sheet_name=sheet, dtype=str)
                    df_list.append(df)
                data = ps.concat(df_list, ignore_index=True) if len(df_list) > 1 else df_list[0]
                data = data.to_spark()
            else:
                raise ValueError("Unsupported file format")
            return data
        except Exception as e:
            logging.exception("Error accessing raw data", exc_info=True)
            raise

    def treat_columns(self, data):
        try:
            # REMOVE DUPLICATED COLUMNS
            valid_columns = [field.name for field in data.schema.fields if not field.name.endswith(tuple(f".{i}" for i in range(1, 10)))]
            data = data.select(valid_columns)
            # REMOVE NULL COLUMNS
            valid_columns = [field.name for field in data.schema.fields if not isinstance(field.dataType, NullType)]
            data = data.select(valid_columns)
            data = data.withColumn("ID",f.sha(f.concat(*[f.coalesce(f.col(c).cast('string'), f.lit('x')) for c in data.columns])))
            return data
        except Exception as e:
            logging.exception("Error treating columns", exc_info=True)
            raise
    
    def treat_data(self,data,year):
        try:
            data = data.withColumn("DATE_PROCESSED",f.current_date()) \
                       .withColumn("YEAR_INFO", f.lit(year)) \
                       .distinct()
            return data
        except Exception as e:
            logging.exception("Error to add ref column", exc_info=True)
            raise
    
    def write_data(self, data, output_path, file_format):
        try:
            data.write.partitionBy("YEAR_INFO")\
                .option("maxRecordsPerFile", 100000)\
                .mode('overwrite')\
                .format(file_format)\
                .parquet(output_path)
        except Exception as e:
            logging.exception("Error writing data", exc_info=True)
            raise

    def all_tables(self):
        output_prefix = 'data/bronze'
        for table in self.bronze_config:
            name = table['name']
            input = table['input']
            output = os.path.join(output_prefix,f"tb_name={table['output']}")
            format = table['format']
            years = table['years']
            logging.info(f"###### BRONZE - {name}")
            # INPUT
            for year in years:
                start_time = time.time()
                data = self.read_data(input,year)
                # TRANSFORM
                data = self.treat_columns(data)
                data = self.treat_data(data,year)
                # WRITE
                self.write_data(data,output,format)
                elapsed = time.time() - start_time
                logging.info(f"###### BRONZE - {name},{year} - DONE IN {elapsed:.2f}s")

        
    def stop_sedona(self):
        self.sedona.stop()