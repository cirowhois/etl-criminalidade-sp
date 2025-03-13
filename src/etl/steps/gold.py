import pyspark.sql.functions as f
import warnings
from utils import log_time, log_subsection, run_tests

warnings.filterwarnings('ignore')

step = "GOLD"

class GoldStep:
    def __init__(self,sedona,logger):
        self.sedona = sedona
        self.logger = logger


    # READ
    @log_time
    def read_data(self, file_path):
        data = self.sedona.read.parquet(file_path)
        return data


    # TREATMENTS
    @log_time
    def treat_data_crimes(self,data):
        data = data.drop('ID_SC', 'CRIMINAL_TYPE_TREATED', 'CRIMINAL_CLASS_TREATED').distinct()
        return data
    
    @log_time
    def treat_data_scs(self,data_crime,data_sc):
        # AGG AND PIVOT
        data = data_crime.groupBy("ID_SC","YEAR_INFO").agg(f.count("ID_CRIME").alias("TOTAL_CRIMES"))
        data = data.join(data_crime.groupBy("ID_SC","YEAR_INFO").pivot("CRIMINAL_TYPE_TREATED").count(), on=["ID_SC","YEAR_INFO"], how="left")
        # SC JOIN
        data = data_sc.join(data, on="ID_SC", how='inner')
        # CRIME INDEX
        data = data.withColumn("TOTAL_CRIME_INDEX",
                               f.when(f.col("YEAR_INFO") < 2022, None) \
                                .when((f.col("POP_TOTAL").isNull()) |
                                      (f.col("TOTAL_CRIMES").isNull()) |
                                      (f.col("POP_TOTAL") == 0), 0) \
                                .otherwise((f.col("TOTAL_CRIMES") / f.col("POP_TOTAL")) * 100000)
                                .cast("double")) \
                    .withColumn("CELLPHONE_CRIME_INDEX",
                               f.when((f.col("POP_TOTAL").isNull()) |
                                      (f.col("FURTO_ROUBO_DE_CELULAR").isNull()) |
                                      (f.col("POP_TOTAL") == 0),0) \
                                .otherwise((f.col("FURTO_ROUBO_DE_CELULAR") / f.col("POP_TOTAL")) * 100000)
                                .cast("double")) \
                    .withColumn("VEHICLE_CRIME_INDEX",
                               f.when((f.col("POP_TOTAL").isNull()) |
                                      (f.col("FURTO_ROUBO_DE_VEICULO").isNull()) |
                                      (f.col("POP_TOTAL") == 0),0) \
                                .otherwise((f.col("FURTO_ROUBO_DE_VEICULO") / f.col("POP_TOTAL")) * 100000)
                                .cast("double")) \
                    .withColumn("ID_SC_CRIME",f.sha2(f.concat_ws("|",*[f.col(x).cast('string') for x in ["ID_SC","YEAR_INFO"]]),256))
        return data

    @log_time
    def treat_data_mun(self,data_crime,data_mun):
        # AGG AND PIVOT
        data = data_crime.groupBy("ID_MUN","YEAR_INFO").agg(f.count("ID_CRIME").alias("TOTAL_CRIMES"))
        data = data.join(data_crime.groupBy("ID_MUN","YEAR_INFO").pivot("CRIMINAL_TYPE_TREATED").count(), on=["ID_MUN","YEAR_INFO"], how="left")
        # MUN JOIN
        data = data_mun.join(data, on="ID_MUN", how='inner')
        # CRIME INDEX
        data = data.withColumn("TOTAL_CRIME_INDEX",
                               f.when(f.col("YEAR_INFO") < 2022, None) \
                                .when((f.col("POP_TOTAL").isNull()) |
                                      (f.col("TOTAL_CRIMES").isNull()) |
                                      (f.col("POP_TOTAL") == 0), 0) \
                                .otherwise((f.col("TOTAL_CRIMES") / f.col("POP_TOTAL")) * 100000)
                                .cast("double")) \
                    .withColumn("CELLPHONE_CRIME_INDEX",
                               f.when((f.col("POP_TOTAL").isNull()) |
                                      (f.col("FURTO_ROUBO_DE_CELULAR").isNull()) |
                                      (f.col("POP_TOTAL") == 0),0) \
                                .otherwise((f.col("FURTO_ROUBO_DE_CELULAR") / f.col("POP_TOTAL")) * 100000)
                                .cast("double")) \
                    .withColumn("VEHICLE_CRIME_INDEX",
                               f.when((f.col("POP_TOTAL").isNull()) |
                                      (f.col("FURTO_ROUBO_DE_VEICULO").isNull()) |
                                      (f.col("POP_TOTAL") == 0),0) \
                                .otherwise((f.col("FURTO_ROUBO_DE_VEICULO") / f.col("POP_TOTAL")) * 100000)
                                .cast("double")) \
                    .withColumn("ID_MUN_CRIME",f.sha(f.concat(*["ID_MUN","YEAR_INFO"])))
        return data


    # WRITE
    @log_time
    def write_data(self, data, output_path, partition_col, max_records=100000):
        data.write.partitionBy(partition_col)\
                  .option("maxRecordsPerFile", max_records)\
                  .mode('overwrite')\
                  .format('parquet')\
                  .parquet(output_path)


    # TESTS
    @log_time
    def test_data(self, output, unique_columns, not_null_columns)->None:
        data = self.sedona.read.parquet(output)
        run_tests(data, self.logger, unique_columns, not_null_columns)



    # GOLD EXECUTORS
    @log_time
    def gold_crimes(self):
        name = "gold_crimes"
        log_subsection(f" {step}: {name}", self.logger)
        
        # READ
        data = self.read_data("data/silver/tb_name=silver_crimes")
        # TREATMENT
        data = self.treat_data_crimes(data)
        # WRITING
        self.write_data(data, f"data/gold/tb_name={name}", "YEAR_INFO", max_records=100000)
        # TESTS
        self.test_data(f"data/gold/tb_name={name}", unique_columns="ID_CRIME", not_null_columns=["ID_CRIME", "geom", "CRIMINAL_CLASS"])

    @log_time
    def gold_crimes_scs(self):
        name = "gold_crimes_scs"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data_crime = self.read_data("data/silver/tb_name=silver_crimes")
        data_sc = self.read_data("data/silver/tb_name=silver_scs").drop("YEAR_INFO")
        # TREATMENT
        data = self.treat_data_scs(data_crime,data_sc)
        # WRITING
        self.write_data(data, f"data/gold/tb_name={name}", "YEAR_INFO", max_records=10000)
        # TESTS
        self.test_data(f"data/gold/tb_name={name}", unique_columns=["ID_SC_CRIME", "ID_SC","YEAR_INFO"], not_null_columns=["ID_SC_CRIME", "ID_SC", "geom", "YEAR_INFO"])

    @log_time
    def gold_crimes_mun(self):
        name = "gold_crimes_mun"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data_crime = self.read_data("data/silver/tb_name=silver_crimes")
        data_mun = self.read_data("data/silver/tb_name=silver_mun").drop("YEAR_INFO")
        # TREATMENT
        data = self.treat_data_mun(data_crime,data_mun)
        # WRITING
        self.write_data(data, f"data/gold/tb_name={name}", "YEAR_INFO", max_records=100)
        # TESTS
        self.test_data(f"data/gold/tb_name={name}", unique_columns=["ID_MUN_CRIME", "ID_MUN","YEAR_INFO"], not_null_columns=["ID_MUN_CRIME", "ID_MUN", "geom", "YEAR_INFO"])

    def tables(self):
        self.gold_crimes()
        self.gold_crimes_scs()
        self.gold_crimes_mun()
