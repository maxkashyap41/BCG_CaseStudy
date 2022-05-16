def count_of_twoWheelers(spark):
    df = spark.read.csv(path="src/datasets/Units_use.csv", sep=",", header=True, quote='"', inferSchema=True)
    print("\n\n\n|--------------- Printing the Schema ---------------|")
    print(df.printSchema())

    count_of_twoWheelers = df.select('CRASH_ID').where((df.UNIT_DESC_ID == 'MOTOR VEHICLE') & ((df.VEH_BODY_STYL_ID == 'MOTORCYCLE') | (df.VEH_BODY_STYL_ID == 'POLICE MOTORCYCLE'))).count()
    
    return count_of_twoWheelers