from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("futbol")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_futbol="dataset.csv"
    df_futbol = spark.read.csv(path_futbol,header=True,inferSchema=True)
    df_futbol.createOrReplaceTempView("futbol")
    query='DESCRIBE futbol'
    spark.sql(query).show(20)

    query="""SELECT home_team FROM futbol WHERE winner_reason == "WIN_REGULAR" ORDER BY `id_match`"""
    df_futbol_winners = spark.sql(query)
    df_futbol_winners.show(20)

    
    spark.stop()