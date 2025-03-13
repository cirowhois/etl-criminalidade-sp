from sedona.spark import *
import pyspark.sql.functions as f
import json
import logging
import sys
import time
import os
import functools


def load_json(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)

# SEDONA CONFIG    
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


# LOGGING CONFIG

def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    log_dir = "etl/logs"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{timestamp}_{name}.log")

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def log_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        logger = getattr(self, "logger", None) 
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            if logger:
                logger.info(f"{func.__name__}: {elapsed_time:.2f}s")
            return result
        except Exception as e:
            elapsed_time = time.time() - start_time
            if logger:
                logger.exception(f"Error in {func.__name__} after {elapsed_time:.2f}s", exc_info=True)
            raise
    return wrapper

def log_section(title, logger, width=50):
    logger.info(f"{title.center(width, '#')}")

def log_subsection(title, logger):
    logger.info(f"{'#' * 10} {title}")


# TESTS

def not_null(data, column):
    null_count = data.filter(f.col(column).isNull()).count()
    assert null_count == 0, f"Column '{column}' has {null_count} null values"

def test_unique(data, columns):
    if isinstance(columns, list):            
        dup_test = "_".join(columns)
        for column in columns:
            data = data.withColumn(column, f.col(column).cast("string"))
        data = data.withColumn(dup_test, f.sha2(f.concat_ws("|",*columns),256))
    else:
        dup_test = columns

    duplicates = data.groupBy(dup_test).count().filter(f.col("count") > 1)
    duplicate_count = duplicates.count()
    assert duplicate_count == 0, f"Column(s) '{dup_test}' has {duplicate_count} duplicated values"

def test_null(data, columns):
    if isinstance(columns, list):
        for column in columns:
            not_null(data, column)
    else:
        not_null(data, columns)

def count_rows(data,logger):
    count = data.count()
    return logger.info(f"rows: {count}")

def run_tests(data, logger, unique_columns, not_null_columns):
    count_rows(data, logger)
    test_unique(data, unique_columns)
    test_null(data, not_null_columns)
    