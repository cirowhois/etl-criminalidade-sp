import os
import shutil
import fiona
import pyspark.sql.functions as f
import pyspark.pandas as ps
import pandas as pd
import warnings
import json
from sedona.spark import *
from pyspark.sql.types import NullType
from utils import log_time, log_subsection, run_tests


warnings.filterwarnings('ignore')

step = "BRONZE"

class BronzeStep:
    def __init__(self,config_path,sedona,logger):
        self.sedona = sedona
        config = self.load_json(config_path)
        self.bronze_config = self.load_config(config)
        self.logger = logger

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

    # READ
    @log_time
    def read_data(self, file, year):
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
            raise 
        return data
    
    # TREATMENTS
    def treat_columns(self, data):
        # REMOVE DUPLICATED COLUMNS
        valid_columns = [field.name for field in data.schema.fields if not field.name.endswith(tuple(f".{i}" for i in range(1, 10)))]
        data = data.select(valid_columns)
        # REMOVE NULL COLUMNS
        valid_columns = [field.name for field in data.schema.fields if not isinstance(field.dataType, NullType)]
        data = data.select(valid_columns)
        data = data.withColumn("ID",f.sha2(f.concat_ws("|",*[f.coalesce(f.col(c).cast('string'), f.lit('x')) for c in data.columns]),256))
        return data

    @log_time
    def treat_data(self,data,year):
        data = self.treat_columns(data)
        data = data.withColumn("DATE_PROCESSED",f.current_date()) \
                   .withColumn("YEAR_INFO", f.lit(year)) \
                   .distinct()
        return data

    # WRITE
    @log_time
    def write_data(self, data, output_path, file_format)->None:
        data.write.partitionBy("YEAR_INFO")\
            .option("maxRecordsPerFile", 100000)\
            .mode('append')\
            .format(file_format)\
            .parquet(output_path)
        
    # TESTS
    @log_time
    def test_data(self, output, unique_columns, not_null_columns)->None:
        data = self.sedona.read.parquet(output)
        run_tests(data, self.logger, unique_columns, not_null_columns)

    # BRONZE EXECUTOR
    @log_time
    def tables(self):
        output_prefix = 'data/bronze'
        if os.path.exists(output_prefix):
            shutil.rmtree(output_prefix)
        for table in self.bronze_config:
            name = table['name']
            input = table['input']
            output = os.path.join(output_prefix,f"tb_name={table['output']}")
            format = table['format']
            years = table['years']
            unique_columns = table['tests']['unique']
            not_null_columns = table['tests']['not_null']
            # INPUT
            for year in years:
                log_subsection(f" {step}: {name}/{year}", self.logger)
                # READ
                data = self.read_data(input,year)
                # TRANSFORM
                data = self.treat_data(data,year)
                # WRITE
                self.write_data(data,output,format)
                # TESTS
                self.test_data(output,unique_columns,not_null_columns)