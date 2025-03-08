import logging
import pyspark.sql.functions as f
import time
import warnings
import unidecode
from sedona.spark import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window




warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)


###### IBGE

class IBGESilver:
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


    def silver_scs_demographics(self):
        name = "silver_scs_demographics"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_scs_demographics")

            # TREATMENT
            data = data.filter("CD_UF = 35")
            data = data.selectExpr(
                "CD_SETOR AS ID_SC",
                "v0001 AS POP_TOTAL",
                "current_date AS DATE_PROCESSED",
                "2022 AS YEAR_INFO"
                ).distinct()

            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=1000)
            
            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_mun_demographics(self):
        name = "silver_mun_demographics"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_scs_demographics")

            # TREATMENT
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
            
            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=100)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_scs_geom(self):
        name = "silver_scs_geom"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_scs_geom")

            # TREATMENT
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

             # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=1000)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_mun_geom(self):
        name = "silver_mun_geom"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_scs_geom")

            # TREATMENT
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

            # WRITING
            self.write_data(data, f"data/gold/tb_name={name}", "YEAR_INFO", max_records=100)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_scs(self):
        name = "silver_scs"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data_demographics = self.read_data("data/silver/tb_name=silver_scs_demographics").drop('DATE_PROCESSED','YEAR_INFO')
            data_geom = self.read_data("data/silver/tb_name=silver_scs_geom").drop('DATE_PROCESSED','YEAR_INFO')

            # TREATMENT
            data = data_geom.join(data_demographics, on='ID_SC',how='left')
            data = data.withColumn('DATE_PROCESSED',f.current_date()) \
                        .withColumn('YEAR_INFO',f.lit(2022))

            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", ["YEAR_INFO","ID_MUN"], max_records=1000)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_mun(self):
        name = "silver_mun"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data_demographics = self.read_data("data/silver/tb_name=silver_mun_demographics").drop('DATE_PROCESSED','YEAR_INFO')
            data_geom = self.read_data("data/silver/tb_name=silver_mun_geom").drop('DATE_PROCESSED','YEAR_INFO')

            # TREATMENT
            data = data_geom.join(data_demographics, on='ID_MUN',how='left')
            data = data.withColumn('DATE_PROCESSED',f.current_date()) \
                       .withColumn('YEAR_INFO',f.lit(2022))

            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", ["YEAR_INFO","ID_MUN"], max_records=100)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def stop_sedona(self):
        self.sedona.stop()



###### SSPSP


# UDF
def text_treatment(texto):
    if texto is not None:
        return unidecode.unidecode(texto)
    return texto
text_treatment_udf = f.udf(text_treatment, StringType())


class SSPSPSilver:
    def __init__(self,sedona):
        self.sedona = sedona

    def read_data(self, file_path):
        try:
            data = self.sedona.read.parquet(file_path)
            return data
        except Exception as e:
            logging.exception("Error reading data", exc_info=True)
            raise

    def treat_columns(self, df):
        columns_to_treat = ['ADDRESS','NEIGHBORHOOD','CRIMINAL_TYPE']
        try:
            for column in columns_to_treat:
                df = df.withColumn(column, text_treatment_udf(f.col(column))) \
                       .withColumn(column, f.regexp_replace(column, "[^a-zA-Z0-9\\s]", "")) \
                       .withColumn(column, f.regexp_replace(column, "\\s+", " ")) \
                       .withColumn(column, f.trim(f.col(column))) \
                       .withColumn(column, f.upper(column))
            return df
        except Exception as e:
            logging.exception("Error to treat columns", exc_info=True)
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

    def silver_crimes_cellphones(self):
        name = "silver_crimes_cellphones"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_crimes_cellphones")

            # TREATMENT
            # FILTER NOT MAPPED/SENSITIVE DATA
            data = data.filter("(LATITUDE IS NOT NULL OR LATITUDE !=0) AND (LONGITUDE IS NOT NULL OR LONGITUDE !=0)") \
                       .filter("LOGRADOURO NOT LIKE '%DIVULGA__O%'")
            data = data.selectExpr("LOGRADOURO AS ADDRESS",
                                   "NUMERO_LOGRADOURO AS ADDRESS_NUMBER",
                                   "BAIRRO AS NEIGHBORHOOD",
                                   "LATITUDE",
                                   "LONGITUDE",
                                   "COALESCE(dayofmonth(DATA_OCORRENCIA_BO),1) AS DAY_INFO",
                                   "MES AS MONTH_INFO",
                                   "ANO AS YEAR_INFO",
                                   "'FURTO ROUBO DE CELULAR' AS CRIMINAL_TYPE")
            # CLEANING COLUMNS
            data = self.treat_columns(data)
            # NEW COLUMNS + GEOMETRY    
            data = data.withColumn("ID",f.hash(f.concat('ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','LATITUDE','LONGITUDE','DAY_INFO','MONTH_INFO','YEAR_INFO'))) \
                       .withColumn('DATE_PROCESSED',f.current_date()) \
                       .withColumn('geom', f.expr("ST_Point(CAST(LONGITUDE AS Decimal(24,20)), CAST(LATITUDE AS Decimal(24,20)))")) \
                       .withColumn('geom', f.expr("ST_SetSRID(geom,4326)"))
            data = data.select('ID','CRIMINAL_TYPE','ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','DAY_INFO','MONTH_INFO','YEAR_INFO','DATE_PROCESSED','geom').distinct()
            
            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=10000)
            
            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise

    def silver_crimes_vehicles(self):
        name = "silver_crimes_vehicles"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_crimes_vehicles")

            # TREATMENT
            # FILTER NOT MAPPED/SENSITIVE DATA
            data = data.filter("(LATITUDE IS NOT NULL OR LATITUDE !=0) AND (LONGITUDE IS NOT NULL OR LONGITUDE !=0)") \
                       .filter("LOGRADOURO NOT LIKE '%DIVULGA__O%'")
            data = data.selectExpr("LOGRADOURO AS ADDRESS",
                                   "NUMERO_LOGRADOURO AS ADDRESS_NUMBER",
                                   "BAIRRO AS NEIGHBORHOOD",
                                   "LATITUDE",
                                   "LONGITUDE",
                                   "COALESCE(dayofmonth(DATA_OCORRENCIA_BO),1) AS DAY_INFO",
                                   "MES AS MONTH_INFO",
                                   "ANO AS YEAR_INFO",
                                   "'FURTO ROUBO DE VEICULO' AS CRIMINAL_TYPE")
            # CLEANING COLUMNS
            data = self.treat_columns(data)
            # NEW COLUMNS + GEOMETRY
            data = data.withColumn("ID",f.hash(f.concat('ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','LATITUDE','LONGITUDE','DAY_INFO','MONTH_INFO','YEAR_INFO'))) \
                       .withColumn('DATE_PROCESSED',f.current_date()) \
                       .withColumn('geom', f.expr("ST_Point(CAST(LONGITUDE AS Decimal(24,20)), CAST(LATITUDE AS Decimal(24,20)))")) \
                       .withColumn('geom', f.expr("ST_SetSRID(geom,4326)"))
            data = data.select('ID','CRIMINAL_TYPE','ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','DAY_INFO','MONTH_INFO','YEAR_INFO','DATE_PROCESSED','geom').distinct()
            
            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=10000)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise
            
    def silver_crimes_others(self):
        name = "silver_crimes_others"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data = self.read_data("data/bronze/tb_name=bronze_crimes_others")

            # TREATMENT
            # FILTER NOT MAPPED/SENSITIVE DATA
            data = data.filter("(LATITUDE IS NOT NULL OR LATITUDE !=0) AND (LONGITUDE IS NOT NULL OR LONGITUDE !=0)") \
                       .filter("LOGRADOURO NOT LIKE '%DIVULGA__O%'")
            data = data.selectExpr("LOGRADOURO AS ADDRESS",
                                   "NUMERO_LOGRADOURO AS ADDRESS_NUMBER",
                                   "BAIRRO AS NEIGHBORHOOD",
                                   "LATITUDE",
                                   "LONGITUDE",
                                   "COALESCE(dayofmonth(DATA_OCORRENCIA_BO),1) AS DAY_INFO",
                                   "MES_ESTATISTICA AS MONTH_INFO",
                                   "ANO_ESTATISTICA AS YEAR_INFO",
                                   "NATUREZA_APURADA AS CRIMINAL_TYPE")
            # CLEANING COLUMNS
            data = self.treat_columns(data)
            # NEW COLUMNS + GEOMETRY
            data = data.withColumn("ID",f.hash(f.concat('ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','LATITUDE','LONGITUDE','DAY_INFO','MONTH_INFO','YEAR_INFO'))) \
                       .withColumn('DATE_PROCESSED',f.current_date()) \
                       .withColumn('geom', f.expr("ST_Point(CAST(LONGITUDE AS Decimal(24,20)), CAST(LATITUDE AS Decimal(24,20)))")) \
                       .withColumn('geom', f.expr("ST_SetSRID(geom,4326)")) \
                       .withColumn('CRIMINAL_TYPE', f.when(f.col("CRIMINAL_TYPE").isin("FURTO DE VEICULO", "ROUBO DE VEICULO"),"FURTO ROUBO DE VEICULO").otherwise(f.col("CRIMINAL_TYPE")))
            data = data.select('ID','CRIMINAL_TYPE','ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','DAY_INFO','MONTH_INFO','YEAR_INFO','DATE_PROCESSED','geom').distinct()
            
            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=10000)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise


    def silver_crimes(self):
        name = "silver_crimes"
        try:
            logging.info(f"###### SILVER - {name}")
            start_time = time.time()

            # READ
            data_cellphones = self.read_data("data/silver/tb_name=silver_crimes_cellphones").withColumn('priority',f.lit(1))
            data_vehicles = self.read_data("data/silver/tb_name=silver_crimes_vehicles").withColumn('priority',f.lit(2))
            data_others = self.read_data("data/silver/tb_name=silver_crimes_others").withColumn('priority',f.lit(3))
            data_scs = self.read_data("data/silver/tb_name=silver_scs_geom")
            

            # TREATMENT
            # DEDUP
            data = data_cellphones.union(data_vehicles).union(data_others)

            windowSpec = Window.partitionBy("ID").orderBy("priority")
            data = data.withColumn("row", f.rank().over(windowSpec)).filter('row = 1').drop('row','priority')
            data = data.withColumn("ID_CRIME",f.sha(f.concat_ws("_", *[f.col(c).cast("string") for c in data.columns]))).drop("ID") \
                       .withColumn("CRIMINAL_TYPE_TREATED", f.upper(f.regexp_replace("CRIMINAL_TYPE", " ", "_"))) \
                       .withColumn("CRIMINAL_CLASS", f.when(f.col("CRIMINAL_TYPE_TREATED") == "PORTE_DE_ARMA", "ARMAS") \
                                                      .when(f.col("CRIMINAL_TYPE_TREATED") == "EXTORSAO_MEDIANTE_SEQUESTRO", "SEQUESTRO") \
                                                      .when(f.col("CRIMINAL_TYPE_TREATED").isin("APREENSAO_DE_ENTORPECENTES", "PORTE_DE_ENTORPECENTES", "TRAFICO_DE_ENTORPECENTES"), "DROGAS") \
                                                      .when(f.col("CRIMINAL_TYPE_TREATED").isin("HOMICIDIO_CULPOSO_OUTROS", "HOMICIDIO_CULPOSO_POR_ACIDENTE_DE_TRANSITO", "HOMICIDIO_DOLOSO", "HOMICIDIO_DOLOSO_POR_ACIDENTE_DE_TRANSITO", "LATROCINIO", "LESAO_CORPORAL_SEGUIDA_DE_MORTE", "TENTATIVA_DE_HOMICIDIO"), "HOMICÍDIO") \
                                                      .when(f.col("CRIMINAL_TYPE_TREATED").isin("LESAO_CORPORAL_DOLOSA", "LESAO_CORPORAL_CULPOSA_POR_ACIDENTE_DE_TRANSITO", "LESAO_CORPORAL_CULPOSA_OUTRAS"), "LESAO CORPORAL") \
                                                      .when(f.col("CRIMINAL_TYPE_TREATED").isin("FURTO_DE_CARGA", "FURTO_OUTROS", "FURTO_ROUBO_DE_CELULAR", "FURTO_ROUBO_DE_VEICULO", "ROUBO_A_BANCO", "ROUBO_DE_CARGA", "ROUBO_OUTROS"), "ROUBO E FURTO") \
                                                      .otherwise("OUTROS")) \
                       .withColumn("CRIMINAL_CLASS_TREATED", f.upper(f.regexp_replace("CRIMINAL_CLASS", " ", "_"))) \
                       .withColumn("DATE_PROCESSED",f.current_date())
            # GEO INFO
            data = data.join(data_scs.select("ID_SC","ID_MUN","MUN","STATE","geom"),on=ST_Intersects(data.geom,data_scs.geom),how='inner').drop(data_scs.geom)
            data = data.selectExpr("ID_CRIME","CRIMINAL_CLASS","CRIMINAL_TYPE","ADDRESS","ADDRESS_NUMBER","NEIGHBORHOOD","DAY_INFO","MONTH_INFO","YEAR_INFO","ID_SC","ID_MUN","MUN","STATE","DATE_PROCESSED","geom","CRIMINAL_CLASS_TREATED","CRIMINAL_TYPE_TREATED")
            
            # WRITING
            self.write_data(data, f"data/silver/tb_name={name}", ["YEAR_INFO","MONTH_INFO"], max_records=10000)

            elapsed = time.time() - start_time
            logging.info(f"###### SILVER - {name} - DONE IN {elapsed:.2f}s")
        except Exception as e:
            logging.exception("Error to treat data", exc_info=True)
            raise
    
    def stop_sedona(self):
        self.sedona.stop()