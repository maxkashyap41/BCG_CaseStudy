def distinct_crashIDs(spark):
    vehicle_df = spark.read.csv(path="src/datasets/Units_use.csv", sep=",", header=True, quote='"', inferSchema=True)
    damages_df = spark.read.csv(path="src/datasets/Damages_use.csv", sep=",", header=True, quote='"', inferSchema=True)

    joined_vehicle_damages_df = vehicle_df.join(damages_df, vehicle_df.CRASH_ID == damages_df.CRASH_ID, 'inner').drop(vehicle_df.CRASH_ID)

    arr_vehicle_damage_scl = ['DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST']
    filter_df_from_vehDamageScl = joined_vehicle_damages_df.filter(joined_vehicle_damages_df.VEH_DMAG_SCL_1_ID.isin(arr_vehicle_damage_scl)).filter(joined_vehicle_damages_df.VEH_DMAG_SCL_2_ID.isin(arr_vehicle_damage_scl)).select('CRASH_ID', 'VEH_BODY_STYL_ID', 'FIN_RESP_TYPE_ID')
    df_filtered_crashIDs_from_insurance = filter_df_from_vehDamageScl.select('CRASH_ID', 'FIN_RESP_TYPE_ID').where((filter_df_from_vehDamageScl.FIN_RESP_TYPE_ID == 'LIABILITY INSURANCE POLICY') | (filter_df_from_vehDamageScl.FIN_RESP_TYPE_ID == 'PROOF OF LIABILITY INSURANCE'))

    count_of_CrashIDs = df_filtered_crashIDs_from_insurance.distinct().count()

    return count_of_CrashIDs