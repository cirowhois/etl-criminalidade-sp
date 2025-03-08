import logging
import pyspark.sql.functions as f
import time
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

class AllGold:
    def __init__(self,sedona):
        self.sedona = sedona

    def read_data(self, file_path):
        try:
            data = self.sedona.read.parquet(file_path)
            return data
        except Exception as e:
            logging.exception("Error reading data", exc_info=True)
            raise

    def write_data(self, df, output_path, partition_col, max_records=100000):
        try:
            df.write.partitionBy(partition_col)\
              .option("maxRecordsPerFile", max_records)\
              .mode('overwrite')\
              .format('parquet')\
              .parquet(output_path)
        except Exception as e:
            logging.exception("Error writing data", exc_info=True)
            raise

    def gold_crimes(self):
        name = "gold_crimes"
        try:
            logging.info(f"###### GOLD - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/silver/tb_name=silver_crimes")

            # TREATMENT
            data = data.drop('ID_SC', 'CRIMINAL_TYPE_TREATED', 'CRIMINAL_CLASS_TREATED').distinct()

            # WRITING
            self.write_data(data, f"data/gold/tb_name={name}", "ID_MUN", max_records=100000)

            elapsed = time.time() - start_time
            logging.info(f"###### GOLD - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def gold_crimes_scs(self):
        name = "gold_crimes_scs"
        try:
            logging.info(f"###### GOLD - {name}")
            start_time = time.time()

            # READ
            data_crime = self.read_data("data/silver/tb_name=silver_crimes")
            data_sc = self.read_data("data/silver/tb_name=silver_scs").drop("YEAR_INFO")

            # TREATMENT
            # AGG AND PIVOT
            data = data_crime.groupBy("ID_SC","YEAR_INFO").agg(f.count("ID_CRIME").alias("TOTAL_CRIMES"))
            data = data.join(data_crime.groupBy("ID_SC","YEAR_INFO").pivot("CRIMINAL_CLASS_TREATED").count(), on=["ID_SC","YEAR_INFO"], how="left")
            # SC JOIN
            data = data_sc.join(data, on="ID_SC", how='left')
            # CRIME INDEX
            data = data.withColumn("TOTAL_CRIME_INDEX",
                                   f.when((f.col("POP_TOTAL").isNull()) |
                                          (f.col("TOTAL_CRIMES").isNull()) |
                                          (f.col("POP_TOTAL") == 0), 0)
                                    .otherwise((f.col("TOTAL_CRIMES") / f.col("POP_TOTAL")) * 100000)
                                    .cast("double")) \
                       .withColumn("ID_SC_CRIME",f.sha(f.concat(*["ID_SC","YEAR_INFO"])))

            # WRITING
            self.write_data(data, f"data/gold/tb_name={name}", "ID_MUN", max_records=10000)

            elapsed = time.time() - start_time
            logging.info(f"###### GOLD - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def gold_crimes_mun(self):
        name = "gold_crimes_mun"
        try:
            logging.info(f"###### GOLD - {name}")
            start_time = time.time()

            # READ
            data_crime = self.read_data("data/silver/tb_name=silver_crimes")
            data_mun = self.read_data("data/silver/tb_name=silver_mun").drop("YEAR_INFO")

            # TREATMENT
            data = data_crime.groupBy("ID_MUN","YEAR_INFO").agg(f.count("ID_CRIME").alias("TOTAL_CRIMES"))
            data = data.join(data_crime.groupBy("ID_MUN","YEAR_INFO").pivot("CRIMINAL_CLASS_TREATED").count(), on=["ID_MUN","YEAR_INFO"], how="left")
            data = data_mun.join(data, on="ID_MUN", how='left')
            data = data.withColumn("TOTAL_CRIME_INDEX",
                                   f.when((f.col("POP_TOTAL").isNull()) |
                                          (f.col("TOTAL_CRIMES").isNull()) |
                                          (f.col("POP_TOTAL") == 0), 0)
                                    .otherwise((f.col("TOTAL_CRIMES") / f.col("POP_TOTAL")) * 100000)
                                    .cast("double")) \
                       .withColumn("ID_MUN_CRIME",f.sha(f.concat(*["ID_MUN","YEAR_INFO"])))

            # WRITING
            self.write_data(data, f"data/gold/tb_name={name}", "ID_MUN", max_records=100)
            
            elapsed = time.time() - start_time
            logging.info(f"###### GOLD - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise
    
    def stop_sedona(self):
        self.sedona.stop()
