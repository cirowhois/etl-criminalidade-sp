import pyspark.sql.functions as f
import warnings
import unidecode
from sedona.spark import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from utils import log_time, log_subsection, run_tests


warnings.filterwarnings('ignore')

step = "SILVER"

###### IBGE

class SilverIbgeStep:
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
    def treat_data_scs_demographics(self,data):
        data = data.filter("CD_UF = 35")
        data = data.selectExpr(
            "CD_SETOR AS ID_SC",
            "v0001 AS POP_TOTAL",
            "current_date AS DATE_PROCESSED",
            "2022 AS YEAR_INFO"
            ).distinct()
        return data
    
    @log_time
    def treat_data_mun_demographics(self,data):
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
        return data
    
    @log_time
    def treat_data_scs_geom(self,data):
        data = data.filter("CD_UF = 35")
        data = data.groupBy("CD_SETOR","CD_MUN","NM_MUN").agg(
            f.expr("ST_Union_Aggr(ST_Transform(geom,'epsg:4674','epsg:4326'))").alias("geom")
            ).distinct()
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
        return data
    
    @log_time
    def treat_data_mun_geom(self,data):
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
        return data
    
    @log_time
    def treat_data_final(self,data_geom,data_demographics,column):
        data = data_geom.join(data_demographics, on=column,how='left')
        data = data.withColumn('DATE_PROCESSED',f.current_date()) \
                   .withColumn('YEAR_INFO',f.lit(2022))
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



    # SILVER IBGE EXECUTORS
    @log_time
    def silver_scs_demographics(self):
        name = "silver_scs_demographics"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_scs_demographics")
        # TREATMENT
        data = self.treat_data_scs_demographics(data)
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=1000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns="ID_SC", not_null_columns="ID_SC")
        
    @log_time
    def silver_mun_demographics(self):
        name = "silver_mun_demographics"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_scs_demographics")
        # TREATMENT
        data = self.treat_data_mun_demographics(data)      
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=100)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns="ID_MUN", not_null_columns="ID_MUN")

    @log_time
    def silver_scs_geom(self):
        name = "silver_scs_geom"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_scs_geom")
        # TREATMENT
        data = self.treat_data_scs_geom(data)
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=1000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID_SC", "geom"], not_null_columns=["ID_SC", "geom"])

    @log_time
    def silver_mun_geom(self):
        name = "silver_mun_geom"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_scs_geom")
        # TREATMENT
        data = self.treat_data_mun_geom(data)
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=100)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID_MUN", "geom"], not_null_columns=["ID_MUN", "geom"])

    @log_time
    def silver_scs(self):
        name = "silver_scs"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data_demographics = self.read_data("data/silver/tb_name=silver_scs_demographics").drop('DATE_PROCESSED','YEAR_INFO')
        data_geom = self.read_data("data/silver/tb_name=silver_scs_geom").drop('DATE_PROCESSED','YEAR_INFO')
        # TREATMENT
        data = self.treat_data_final(data_geom,data_demographics,column="ID_SC")
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=1000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID_SC", "geom"], not_null_columns=["ID_SC", "geom"])

    @log_time
    def silver_mun(self):
        name = "silver_mun"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data_demographics = self.read_data("data/silver/tb_name=silver_mun_demographics").drop('DATE_PROCESSED','YEAR_INFO')
        data_geom = self.read_data("data/silver/tb_name=silver_mun_geom").drop('DATE_PROCESSED','YEAR_INFO')
        # TREATMENT
        data = self.treat_data_final(data_geom,data_demographics,"ID_MUN")
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", ["YEAR_INFO","ID_MUN"], max_records=100)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID_MUN", "geom"], not_null_columns=["ID_MUN", "geom"])

    @log_time
    def tables(self):
        self.silver_scs_demographics()
        self.silver_mun_demographics()
        self.silver_scs_geom()
        self.silver_mun_geom()
        self.silver_scs()
        self.silver_mun()

###### SSPSP


# UDFS
def text_treatment(texto):
    if texto is not None:
        return unidecode.unidecode(texto)
    return texto
text_treatment_udf = f.udf(text_treatment, StringType())

def fix_latlong(value_str):
    if value_str is None:
        return None
    s = value_str.strip()
    sign = ''
    if s.startswith('-'):
        sign = '-'
        s = s[1:]
    if '.' in s:
        return sign + s
    if len(s) <= 2:
        return sign + s
    s = s[:2] + '.' + s[2:]
    return sign + s
fix_latlong_udf = f.udf(fix_latlong, StringType())


class SilverSspspStep:
    def __init__(self,sedona,logger):
        self.sedona = sedona
        self.logger = logger


    # READ
    @log_time
    def read_data(self, file_path):
        data = self.sedona.read.parquet(file_path)
        return data
    

    # TREATMENTS
    def treat_columns(self, data):
        columns_to_treat = ['ADDRESS','NEIGHBORHOOD','CRIMINAL_TYPE']
        for column in columns_to_treat:
            data = data.withColumn(column, text_treatment_udf(f.col(column))) \
                       .withColumn(column, f.regexp_replace(column, "[^a-zA-Z0-9\\s]", "")) \
                       .withColumn(column, f.regexp_replace(column, "\\s+", " ")) \
                       .withColumn(column, f.trim(f.col(column))) \
                       .withColumn(column, f.upper(column))
        return data

    @log_time
    def treat_data(self,data,crime_type,month,year):
        crime = f.lit(crime_type)
        # FILTER NOT MAPPED/SENSITIVE DATA
        data = data.filter("LATITUDE IS NOT NULL AND LATITUDE !=0 AND LONGITUDE IS NOT NULL AND LONGITUDE !=0") \
                   .filter("LOGRADOURO NOT LIKE '%DIVULGA__O%'")
        data = data.select(
                        f.col("LOGRADOURO").alias("ADDRESS"),
                        f.col("NUMERO_LOGRADOURO").alias("ADDRESS_NUMBER"),
                        f.col("BAIRRO").alias("NEIGHBORHOOD"),
                        fix_latlong_udf(f.col("LATITUDE")).alias("LATITUDE"),
                        fix_latlong_udf(f.col("LONGITUDE")).alias("LONGITUDE"),
                        f.coalesce(f.dayofmonth("DATA_OCORRENCIA_BO"), f.lit(1)).alias("DAY_INFO"),
                        f.col(month).alias("MONTH_INFO"),
                        f.col(year).alias("YEAR_INFO"),
                        crime.alias("CRIMINAL_TYPE")
                        )
        # CLEANING COLUMNS
        data = self.treat_columns(data)
        # NEW COLUMNS + GEOMETRY    
        cols = ['ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','LATITUDE','LONGITUDE','DAY_INFO','MONTH_INFO','YEAR_INFO']
        data = data.withColumn("ID",f.sha2(f.concat_ws('_',*[f.col(column).cast('string') for column in cols]),256)) \
                   .withColumn('DATE_PROCESSED',f.current_date()) \
                   .withColumn('geom', f.expr("ST_Point(CAST(LONGITUDE AS Decimal(24,20)), CAST(LATITUDE AS Decimal(24,20)))")) \
                   .withColumn('geom', f.expr("ST_SetSRID(geom,4326)"))
        data = data.select('ID','CRIMINAL_TYPE','ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','DAY_INFO','MONTH_INFO','YEAR_INFO','DATE_PROCESSED','geom').distinct()
        return data

    @log_time
    def treat_data_others(self,data,month,year):
        # FILTER NOT MAPPED/SENSITIVE DATA
        data = data.filter("LATITUDE IS NOT NULL AND LATITUDE !=0 AND LONGITUDE IS NOT NULL AND LONGITUDE !=0") \
                   .filter("LOGRADOURO NOT LIKE '%DIVULGA__O%'")
        data = data.select(
                        f.col("LOGRADOURO").alias("ADDRESS"),
                        f.col("NUMERO_LOGRADOURO").alias("ADDRESS_NUMBER"),
                        f.col("BAIRRO").alias("NEIGHBORHOOD"),
                        fix_latlong_udf(f.col("LATITUDE")).alias("LATITUDE"),
                        fix_latlong_udf(f.col("LONGITUDE")).alias("LONGITUDE"),
                        f.coalesce(f.dayofmonth("DATA_OCORRENCIA_BO"), f.lit(1)).alias("DAY_INFO"),
                        f.col(month).alias("MONTH_INFO"),
                        f.col(year).alias("YEAR_INFO"),
                        f.when(f.upper(f.col('NATUREZA_APURADA')).isin('FURTO DE VEÍCULO','ROUBO DE VEÍCULO'),'FURTO ROUBO DE VEICULO') \
                            .otherwise(f.col('NATUREZA_APURADA')).alias("CRIMINAL_TYPE")
                        )
        # CLEANING COLUMNS
        data = self.treat_columns(data)
        # NEW COLUMNS + GEOMETRY    
        cols = ['ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','LATITUDE','LONGITUDE','DAY_INFO','MONTH_INFO','YEAR_INFO']
        data = data.withColumn("ID",f.sha2(f.concat_ws('_',*[f.col(column).cast('string') for column in cols]),256)) \
                   .withColumn('DATE_PROCESSED',f.current_date()) \
                   .withColumn('geom', f.expr("ST_Point(CAST(LONGITUDE AS Decimal(24,20)), CAST(LATITUDE AS Decimal(24,20)))")) \
                   .withColumn('geom', f.expr("ST_SetSRID(geom,4326)"))
        data = data.select('ID','CRIMINAL_TYPE','ADDRESS','ADDRESS_NUMBER','NEIGHBORHOOD','DAY_INFO','MONTH_INFO','YEAR_INFO','DATE_PROCESSED','geom').distinct()
        return data

    @log_time
    def treat_data_crimes(self,data_cellphones,data_vehicles,data_others,data_scs):
        # DEDUP
        data = data_cellphones.union(data_vehicles).union(data_others)
        windowSpec = Window.partitionBy("ID").orderBy("priority")
        data = data.withColumn("row", f.rank().over(windowSpec)).filter('row = 1').drop('row','priority')
        # COLUMNS
        data = data.withColumn("ID_CRIME",f.sha2(f.concat_ws("_", *[f.col(c).cast("string") for c in data.columns]),256)).drop("ID") \
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



    # SILVER SSPSP EXECUTORS

    @log_time
    def silver_crimes_cellphones(self):
        name = "silver_crimes_cellphones"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_crimes_cellphones")
        # TREATMENT
        data = self.treat_data(data,crime_type="'FURTO ROUBO DE CELULAR'",month='MES',year='ANO')
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=10000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID","CRIMINAL_TYPE"], not_null_columns=["ID", "geom"])

    @log_time
    def silver_crimes_vehicles(self):
        name = "silver_crimes_vehicles"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_crimes_vehicles")
        # TREATMENT
        data = self.treat_data(data,crime_type="'FURTO ROUBO DE VEICULO'",month='MES',year='ANO')
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=10000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID","CRIMINAL_TYPE"], not_null_columns=["ID", "geom"])

    @log_time
    def silver_crimes_others(self):
        name = "silver_crimes_others"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data = self.read_data("data/bronze/tb_name=bronze_crimes_others")
        # TREATMENT
        data = self.treat_data_others(data,month='MES_ESTATISTICA',year='ANO_ESTATISTICA')
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", "YEAR_INFO", max_records=10000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns=["ID","CRIMINAL_TYPE"], not_null_columns=["ID", "geom"])


    @log_time
    def silver_crimes(self):
        name = "silver_crimes"
        log_subsection(f" {step}: {name}", self.logger)

        # READ
        data_cellphones = self.read_data("data/silver/tb_name=silver_crimes_cellphones").withColumn('priority',f.lit(1))
        data_vehicles = self.read_data("data/silver/tb_name=silver_crimes_vehicles").withColumn('priority',f.lit(2))
        data_others = self.read_data("data/silver/tb_name=silver_crimes_others").withColumn('priority',f.lit(3))
        data_scs = self.read_data("data/silver/tb_name=silver_scs_geom")
        # TREATMENT
        data = self.treat_data_crimes(data_cellphones,data_vehicles,data_others,data_scs)
        # WRITING
        self.write_data(data, f"data/silver/tb_name={name}", ["YEAR_INFO","MONTH_INFO"], max_records=10000)
        # TESTS
        self.test_data(f"data/silver/tb_name={name}", unique_columns="ID_CRIME", not_null_columns=["ID_CRIME", "geom", "CRIMINAL_CLASS", "ID_SC", "ID_MUN"])


    @log_time
    def tables(self):
        self.silver_crimes_cellphones()
        self.silver_crimes_vehicles()
        self.silver_crimes_others()
        self.silver_crimes()