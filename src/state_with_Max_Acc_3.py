def count_number_of_female(spark):
    df = spark.read.csv(path='src/datasets/Primary_Person_use.csv', sep=',', header=True, quote='"', inferSchema=True)

    female_df = df.select('PRSN_GNDR_ID', 'DRVR_LIC_STATE_ID').where(df.PRSN_GNDR_ID == 'FEMALE')
    state_counts_df = female_df.groupBy('DRVR_LIC_STATE_ID').count()
    maxCount_df = state_counts_df.agg({'count': 'max'}).withColumnRenamed('max(count)', 'count')
    state_with_highest_count_df = state_counts_df.join(maxCount_df, ['count'], 'inner').select('DRVR_LIC_STATE_ID')
    
    return state_with_highest_count_df