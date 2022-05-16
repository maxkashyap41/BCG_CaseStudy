def count_number_of_male(spark):
    df = spark.read.csv(path='src/datasets/Primary_Person_use.csv', sep=',', header=True, quote='"', inferSchema=True)
    print("\n\n\n|--------------- Printing the Schema ---------------|")
    print(df.printSchema())

    count_of_male_accident = df.select('PRSN_GNDR_ID').where((df.PRSN_GNDR_ID == 'MALE') & (df.DEATH_CNT == 1)).count()
    return count_of_male_accident
