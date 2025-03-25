from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TurkishMusicAnalysis")\
        .getOrCreate()

    print("Reading Turkish_Music_Mood_Recognition.csv ...")
    file_path = "Turkish_Music_Mood_Recognition.csv"
    df_music = spark.read.csv(file_path, header=True, inferSchema=True)

    df_music.printSchema()
    
    df_music.createOrReplaceTempView("music")
    
    query = 'SELECT * FROM music LIMIT 20'
    spark.sql(query).show()
    
    query = 'SELECT Class, COUNT(*) AS count FROM music GROUP BY Class ORDER BY count DESC'
    df_class_count = spark.sql(query)
    df_class_count.show()
    
    # Filtrar por una métrica específica, por ejemplo, valores altos de Pulse Clarity
    query = 'SELECT Class, _Pulseclarity_Mean FROM music WHERE _Pulseclarity_Mean > 0.5 ORDER BY _Pulseclarity_Mean DESC'
    df_pulse_clarity = spark.sql(query)
    df_pulse_clarity.show()

    # Crear directorio de salida si no existe
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)

    # Guardar el DataFrame en un archivo JSON
    output_path = os.path.join(output_dir, "pulse_clarity.json")
    df_pulse_clarity.coalesce(1).write.mode("overwrite").json(output_dir)

    # Leer el archivo JSON generado y guardarlo como un solo archivo
    json_files = [f for f in os.listdir(output_dir) if f.startswith("part-")]
    if json_files:
        os.rename(os.path.join(output_dir, json_files[0]), output_path)

    print(f"Datos guardados en `{output_path}`")

    spark.stop()
