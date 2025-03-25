from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TurkishMusicAnalysis")\
        .getOrCreate()

    print("Reading Turkish_Music_Mood_Recognition.csv ...")
    file_path = "Turkish_Music_Mood_Recognition.csv"
    df_music = spark.read.csv(file_path, header=True, inferSchema=True)

    df_music.createOrReplaceTempView("music")

    query = 'DESCRIBE music'
    spark.sql(query).show(20)

    query = 'SELECT Class, COUNT(*) AS count FROM music GROUP BY Class ORDER BY count DESC'
    df_class_count = spark.sql(query)
    df_class_count.show()

    # Filtrar por valores altos de Pulse Clarity
    query = 'SELECT Class, _Pulseclarity_Mean FROM music WHERE _Pulseclarity_Mean > 0.5 ORDER BY _Pulseclarity_Mean DESC'
    df_pulse_clarity = spark.sql(query)
    df_pulse_clarity.show(20)

    # Guardar los resultados en JSONN
    results = df_pulse_clarity.toJSON().collect()
    df_pulse_clarity.write.mode("overwrite").json("results")

    with open('results/pulse_clarity.json', 'w') as file:
        json.dump(results, file)

    print("Datos guardados en `results/pulse_clarity.json`")

    spark.stop()
