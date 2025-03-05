import os
import logging
import pyspark.sql.functions as f
import time
import warnings
import json
from sedona.spark import *


warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

class IBGESilverPrep:
    def __init__(self,config_path):
        self.__config_path = config_path

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)
        
    def load_sedona_config(self,json):
        sedona_config = json['spark_config']
        spark_master = sedona_config['spark_master']
        spark_driver_memory = sedona_config['spark_driver_memory']
        spark_executor_memory = sedona_config['spark_executor_memory']
        spark_executor_instances = sedona_config['spark_executor_instances']
        spark_driver_cores = sedona_config['spark_driver_cores']
        spark_executor_cores = sedona_config['spark_executor_cores']
        sedona_jars = sedona_config['sedona_jars']['packages']
        sedona_repo = sedona_config['sedona_jars']['repositories']
        return spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo

    def s_config(self,name,spark_master,spark_driver_memory,spark_executor_memory,spark_executor_instances,spark_driver_cores,spark_executor_cores,sedona_jars,sedona_repo) -> SedonaContext:
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

    def read_data(self,sedona,file):
        try:
            # READING DATA
            logging.info(f"Reading data from {file.split('=')[-1]}")
            stage_time = time.time()
            data = sedona.read.parquet(file,header=True,inferSchema=False)
            elapsed_time = time.time() - stage_time
            logging.info(f"Data read in {elapsed_time}s")
            return data
        except Exception as e:
            logging.exception("Error to access data", exc_info=True)
            raise

    def silver_scs_demographics(self):
        name = "silver_scs_demographics"
        
        try:
            logging.info(f"Generating {name} data")
            start_time = time.time()

            # SET SEDONA
            config = self.load_json(self.__config_path)
            spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(config)
            sedona_configs = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_configs)

            # READ
            data = self.read_data(sedona,f"data/bronze/tb_name=bronze_scs_demographics")

            # TREATMENT
            stage_time = time.time()
            data = data.filter("CD_UF = 35")
            data = data.selectExpr(
                "CD_SETOR AS ID_SC",
                "v0001 AS POP_TOTAL",
                "current_date AS DATE_PROCESSED",
                "2022 AS YEAR_INFO"
                ).distinct()
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} treated in {elapsed_time}s")
            
            # WRITING
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO").option("maxRecordsPerFile",30000).mode('append').format('parquet').parquet(f'data/silver/tb_name={name}')
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} written in {elapsed_time}")

            elapsed_time = time.time() - start_time
            logging.info(f"{name} created in {elapsed_time}s")
            sedona.stop()
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_mun_demographics(self):
        name = "silver_mun_demographics"

        try:
            logging.info(f"Generating {name} data")
            start_time = time.time()

            # SET SEDONA
            config = self.load_json(self.__config_path)
            spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(config)
            sedona_configs = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_configs)

            # READ
            data = self.read_data(sedona,f"data/bronze/tb_name=bronze_scs_demographics")

            # TREATMENT
            stage_time = time.time()
            data = data.filter("CD_UF = 35")
            data = data.groupBy("CD_MUN").agg(
                f.sum("v0001").alias("POP_TOTAL"),
                f.current_date().alias("DATE_PROCESSED"),
                f.lit(2022).alias("YEAR_INFO")
            ).selectExpr(
                "CD_MUN AS ID_MUN",
                "CAST(POP_TOTAL AS INT) AS POP_TOTAL",
                "DATE_PROCESSED",
                "YEAR_INFO"
            ).distinct()
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} treated in {elapsed_time}s")
            
            # WRITING
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO").option("maxRecordsPerFile",100).mode('append').format('parquet').parquet(f'data/silver/tb_name={name}')
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} written in {elapsed_time}")

            elapsed_time = time.time() - start_time
            logging.info(f"{name} created in {elapsed_time}s")
            sedona.stop()
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_scs_geom(self):
        name = "silver_scs_geom"

        try:
            logging.info(f"Generating {name} data")
            start_time = time.time()

            # SET SEDONA
            config = self.load_json(self.__config_path)
            spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(config)
            sedona_configs = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_configs)

            # READ
            data = self.read_data(sedona,f"data/bronze/tb_name=bronze_scs_geom")

            # TREATMENT
            stage_time = time.time()
            data = data.filter("CD_UF = 35")
            data = data.selectExpr(
                "CD_SETOR AS ID_SC",
                "CD_MUN AS ID_MUN",
                "NM_MUN AS MUN",
                "'SP' AS STATE",
                "ST_Area(ST_Transform(geom,'epsg:4674','epsg:3857'))/1000000 AS AREA_KM2",
                "ST_Transform(geom,'epsg:4674','epsg:4326') AS geom",
                "current_date AS DATE_PROCESSED",
                "2022 AS YEAR_INFO"
                ).distinct()
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} treated in {elapsed_time}s")
            
            # WRITING
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO","ID_MUN").option("maxRecordsPerFile",30000).mode('append').format('parquet').parquet(f'data/silver/tb_name={name}')
            elapsed_time = time.time() - stage_time

            elapsed_time = time.time() - start_time
            logging.info(f"{name} created in {elapsed_time}s")
            sedona.stop()
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_mun_geom(self):
        name = "silver_mun_geom"

        try:
            logging.info(f"Generating {name} data")
            start_time = time.time()

            # SET SEDONA
            config = self.load_json(self.__config_path)
            spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(config)
            sedona_configs = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_configs)

            # READ
            data = self.read_data(sedona,f"data/bronze/tb_name=bronze_scs_geom")

            # TREATMENT
            stage_time = time.time()
            data = data.filter("CD_UF = 35")
            data = data.groupBy("CD_MUN","NM_MUN").agg(
                f.expr("ST_Union_Aggr(ST_Transform(geom,'epsg:4674','epsg:4326'))").alias("geom"),
                f.current_date().alias("DATE_PROCESSED"),
                f.lit(2022).alias("YEAR_INFO"),
                f.lit('SP').alias("STATE")
            ).selectExpr(
                "CD_MUN AS ID_MUN",
                "NM_MUN AS MUN",
                "STATE",
                "ST_Area(ST_Transform(geom,'epsg:4674','epsg:3857'))/1000000 AS AREA_KM2",
                "geom",
                "DATE_PROCESSED",
                "YEAR_INFO"
            ).distinct()
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} treated in {elapsed_time}s")
            
            # WRITING
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO","ID_MUN").option("maxRecordsPerFile",100).mode('append').format('parquet').parquet(f'data/silver/tb_name={name}')
            elapsed_time = time.time() - stage_time

            elapsed_time = time.time() - start_time
            logging.info(f"{name} created in {elapsed_time}s")
            sedona.stop()
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

class IBGESilverFinal:
    def __init__(self,config_path):
        self.__config_path = config_path

    def load_json(self,config_path):
        with open(config_path, 'r') as file:
            return json.load(file)
        
    def load_sedona_config(self,json):
        sedona_config = json['spark_config']
        spark_master = sedona_config['spark_master']
        spark_driver_memory = sedona_config['spark_driver_memory']
        spark_executor_memory = sedona_config['spark_executor_memory']
        spark_executor_instances = sedona_config['spark_executor_instances']
        spark_driver_cores = sedona_config['spark_driver_cores']
        spark_executor_cores = sedona_config['spark_executor_cores']
        sedona_jars = sedona_config['sedona_jars']['packages']
        sedona_repo = sedona_config['sedona_jars']['repositories']
        return spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo

    def s_config(self,name,spark_master,spark_driver_memory,spark_executor_memory,spark_executor_instances,spark_driver_cores,spark_executor_cores,sedona_jars,sedona_repo) -> SedonaContext:
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
                                .getOrCreate()
        except Exception as e:
            logging.exception("Error creating SedonaContext", exc_info=True)
            raise

    def sedona_context(self,s_config):
        return SedonaContext.create(s_config)

    def read_data(self,sedona,file):
        try:
            # READING DATA
            logging.info(f"Reading data from {file.split('=')[-1]}")
            stage_time = time.time()
            data = sedona.read.parquet(file,header=True,inferSchema=False)
            elapsed_time = time.time() - stage_time
            logging.info(f"Data read in {elapsed_time}s")
            return data
        except Exception as e:
            logging.exception("Error to access data", exc_info=True)
            raise

    def silver_scs(self):
        name = "silver_scs"
        
        try:
            logging.info(f"Generating {name} data")
            start_time = time.time()

            # SET SEDONA
            config = self.load_json(self.__config_path)
            spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(config)
            sedona_configs = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_configs)

            # READ
            data_demographics = self.read_data(sedona,f"data/silver/tb_name=silver_scs_demographics").drop('DATE_PROCESSED','YEAR_INFO')
            data_geom = self.read_data(sedona,f"data/silver/tb_name=silver_scs_geom").drop('DATE_PROCESSED','YEAR_INFO')

            # TREATMENT
            stage_time = time.time()
            data = data_geom.join(data_demographics, on='ID_SC',how='left')
            data = data.withColumn('DATE_PROCESSED',f.current_date()) \
                        .withColumn('YEAR_INFO',f.lit(2022))
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} treated in {elapsed_time}s")
            
            # WRITING
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO","ID_MUN").option("maxRecordsPerFile",30000).mode('append').format('parquet').parquet(f'data/silver/tb_name={name}')
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} written in {elapsed_time}")

            elapsed_time = time.time() - start_time
            logging.info(f"{name} created in {elapsed_time}s")
            sedona.stop()
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_mun(self):
        name = "silver_mun"
        
        try:
            logging.info(f"Generating {name} data")
            start_time = time.time()

            # SET SEDONA
            config = self.load_json(self.__config_path)
            spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = self.load_sedona_config(config)
            sedona_configs = self.s_config(name,spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo)
            sedona = self.sedona_context(sedona_configs)

            # READ
            data_demographics = self.read_data(sedona,f"data/silver/tb_name=silver_mun_demographics").drop('DATE_PROCESSED','YEAR_INFO')
            data_geom = self.read_data(sedona,f"data/silver/tb_name=silver_mun_geom").drop('DATE_PROCESSED','YEAR_INFO')

            # TREATMENT
            stage_time = time.time()
            data = data_geom.join(data_demographics, on='ID_MUN',how='left')
            data = data.withColumn('DATE_PROCESSED',f.current_date()) \
                        .withColumn('YEAR_INFO',f.lit(2022))
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} treated in {elapsed_time}s")
            
            # WRITING
            stage_time = time.time()
            data.write.partitionBy("YEAR_INFO","ID_MUN").option("maxRecordsPerFile",100).mode('append').format('parquet').parquet(f'data/silver/tb_name={name}')
            elapsed_time = time.time() - stage_time
            logging.info(f"{name} written in {elapsed_time}")

            elapsed_time = time.time() - start_time
            logging.info(f"{name} created in {elapsed_time}s")
            sedona.stop()
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise