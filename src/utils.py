from sedona.spark import *
import json

def load_json(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)
    
def load_sedona_config(dict):
    sedona_config = dict['spark_config']
    spark_master = sedona_config['spark_master']
    spark_driver_memory = sedona_config['spark_driver_memory']
    spark_executor_memory = sedona_config['spark_executor_memory']
    spark_executor_instances = sedona_config['spark_executor_instances']
    spark_driver_cores = sedona_config['spark_driver_cores']
    spark_executor_cores = sedona_config['spark_executor_cores']
    sedona_jars = sedona_config['sedona_jars']['packages']
    sedona_repo = sedona_config['sedona_jars']['repositories']
    return spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo

def get_sedona(json_file):
    dict = load_json(json_file)
    spark_master, spark_driver_memory, spark_executor_memory, spark_executor_instances, spark_driver_cores, spark_executor_cores, sedona_jars, sedona_repo = load_sedona_config(dict)

    config = SedonaContext.builder().appName("CRIMINAL DATA - ETL") \
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
    return SedonaContext.create(config)

sedona = get_sedona('config.json')