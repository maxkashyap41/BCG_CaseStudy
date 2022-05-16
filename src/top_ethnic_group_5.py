from functools import reduce
from pyspark.sql import DataFrame


def ethnic_wise_breakdown(p_df, v_df, ethnic_race):
    df_1 = p_df.join(v_df, p_df.CRASH_ID == v_df.CRASH_ID, 'inner')
    df_2 = df_1.select('PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID')
    df_ethnic_grp = df_2.select('PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID').where(df_2.PRSN_ETHNICITY_ID == f"{ethnic_race}")

    df_count = df_ethnic_grp.groupBy('PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID').count()
    # print(df_count.show())
    df_maxCount = df_count.agg({'count': 'max'}).withColumnRenamed('max(count)', 'count')
    ethnic_vehicle_model_df = df_count.join(df_maxCount, ['count'], 'inner').select('PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID')

    return ethnic_vehicle_model_df



def unique_body_styles(spark):
    person_df = spark.read.csv(path='src/datasets/Primary_Person_use.csv', sep=',', header=True, quote='"', inferSchema=True)
    vehicle_df = spark.read.csv(path="src/datasets/Units_use.csv", sep=",", header=True, quote='"', inferSchema=True)
    
    ethnics_df = person_df.select('PRSN_ETHNICITY_ID').distinct()
    data_collect = ethnics_df.collect()
    
    array_of_dfs = []
    for row in data_collect:
        items = row['PRSN_ETHNICITY_ID']
        vehicle_model_dfs = ethnic_wise_breakdown(person_df, vehicle_df, items)
        array_of_dfs.append(vehicle_model_dfs)
    
    merged_ethnic_df = reduce(DataFrame.unionAll, array_of_dfs)

    return merged_ethnic_df

    

    