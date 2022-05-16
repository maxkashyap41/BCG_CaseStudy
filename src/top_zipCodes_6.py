from pyspark.sql.functions import col

def get_ZipCodes(spark):
    person_df = spark.read.csv(path='src/datasets/Primary_Person_use.csv', sep=',', header=True, quote='"', inferSchema=True)
    vehicle_df = spark.read.csv(path="src/datasets/Units_use.csv", sep=",", header=True, quote='"', inferSchema=True)

    df_1 = person_df.join(vehicle_df, person_df.CRASH_ID == vehicle_df.CRASH_ID, 'inner')
    # print(len(df_1.columns))
    driverZip_df = df_1.select('VEH_BODY_STYL_ID', 'PRSN_ALC_RSLT_ID', 'DRVR_ZIP').where(((df_1.VEH_BODY_STYL_ID == 'PASSENGER CAR, 4-DOOR') | (df_1.VEH_BODY_STYL_ID == 'PASSENGER CAR, 2-DOOR')) & (df_1.PRSN_ALC_RSLT_ID == 'Positive'))
    # print(driverZip_df.count())
    
    grpByCount_driverZip_df = driverZip_df.groupBy('DRVR_ZIP').count()
    sorted_df = grpByCount_driverZip_df.orderBy(col('count').desc())
    zipCode_df = sorted_df.limit(6).select('DRVR_ZIP').where(sorted_df.DRVR_ZIP != 'null')

    return zipCode_df