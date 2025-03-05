from etl.transformation.bronze__all_data import AllBronze
from etl.transformation.silver__geo_demog import IBGESilverPrep,IBGESilverFinal
from etl.transformation.silver__crimes import SSPSPSilverPrep

# LOAD ETL CLASSES
bronze = AllBronze
silver_ibge_prep = IBGESilverPrep
silver_ibge_final = IBGESilverFinal
silver_sspsp_prep = SSPSPSilverPrep


# LOAD CONFIG
config_path = 'config.json'



# BRONZE >> 
    # FROM RAW TO PARQUET
    # DATE PROCESS COLUMN
    # YEAR INFO COLUMN
#bronze(config_path).all_tables()

# SILVER >>
    # GEOM COLUMN
    # FILTER NOT MAPPED ITEMS
    # SET 4326 SRID
    # DEDUP INFO
    # SET SCHEMA FOR IBGE AND SSPSP DATA
#silver_ibge_prep(config_path).silver_scs_demographics()
#silver_ibge_prep(config_path).silver_mun_demographics()
#silver_ibge_prep(config_path).silver_scs_geom()
#silver_ibge_prep(config_path).silver_mun_geom()
#
#silver_ibge_final(config_path).silver_scs()
#silver_ibge_final(config_path).silver_mun()


silver_sspsp_prep(config_path).silver_crimes_cellphones()
silver_sspsp_prep(config_path).silver_crimes_vehicles()
silver_sspsp_prep(config_path).silver_crimes_others()