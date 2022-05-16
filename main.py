import os
import sys

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')


from pyspark.sql import SparkSession
from src import (
    male_car_accidents_1,
    two_wheeler_crashes_2,
    state_with_Max_Acc_3,
    top_ethnic_group_5,
    top_zipCodes_6,
    count_distinct_crashIDs_7
)


if __name__ == "__main__":

    spark = SparkSession.builder.master("local").appName("CarCrash_Analyzer").getOrCreate()

    print("\n--------------- Choose your options to look into the Analysis ---------------")

    while True:
        print("\n\n\n1.   Analysis 1.")
        print("2.   Analysis 2.")
        print("3.   Analysis 3.")
        print("4.   Analysis 4.")
        print("5.   Analysis 5.")
        print("6.   Analysis 6.")
        print("7.   Analysis 7.")
        print("8.   Analysis 8.")
        print("9.   Exit.")


        choice = int(input("\nEnter your choice: "))

        if choice == 1:
            result = male_car_accidents_1.count_number_of_male(spark)
            print("\n\nThe number of accidents in which number of persons killed are male: ", result)
        elif choice == 2:
            result = two_wheeler_crashes_2.count_of_twoWheelers(spark)
            print("\n\nThe number of two wheelers are booked for crashes: ", result)
        elif choice == 3:
            resultant_df = state_with_Max_Acc_3.count_number_of_female(spark)
            print("\n\nState that has highest number of accidents in which females are involved: ")
            print(resultant_df.show())
        elif choice == 4:
            pass
        elif choice == 5:
            resultant_df = top_ethnic_group_5.unique_body_styles(spark)
            print("\n\nFor all the body styles involved in crashes, the top ethnic user group of each unique body style are: ")
            print(resultant_df.show())
        elif choice == 6:
            resultant_df = top_zipCodes_6.get_ZipCodes(spark)
            print("\n\nAmong the crashed cars, the Top 5 Zip Codes with highest number of crashes with alcohols: ")
            print(resultant_df.show())
        elif choice == 7:
            resultant_count = count_distinct_crashIDs_7.distinct_crashIDs(spark)
            print("\n\nCount of Distinct Crash IDs where Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance: ", resultant_count)
        elif choice == 8:
            pass
        elif choice == 9:
            spark.stop()
            print("Spark Session Closed...")
            print("Going for the Exit!")
            break
        else:
            print("\nWrong Choice.... Try Again!")
