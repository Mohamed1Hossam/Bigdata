
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as sum_, count, explode
from pyspark.sql.types import *
import json
from pyspark.sql.functions import col,lit , coalesce
from pyspark.sql.functions import col, from_json, decode,when

event_schema = StructType([
    StructField("HomeScore", IntegerType(), True),
    StructField("H_Throw_in", DoubleType(), True),
    StructField("A_Throw_in", DoubleType(), True),
    StructField("H_Yellow_Cards", DoubleType(), True),
    StructField("A_Yellow_Cards", DoubleType(), True),
    StructField("League", StringType(), True),
    StructField("Home", StringType(), True),
    StructField("Away", StringType(), True),
    StructField("INC", ArrayType(StructType([
        StructField("raw", StringType(), True)
    ]))),
    StructField("Round", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("H_Score", DoubleType(), True),
    StructField("A_Score", DoubleType(), True),
    StructField("HT_H_Score", DoubleType(), True),
    StructField("HT_A_Score", DoubleType(), True),
    StructField("WIN", StringType(), True),
    StructField("H_BET", DoubleType(), True),
    StructField("X_BET", DoubleType(), True),
    StructField("A_BET", DoubleType(), True),
    StructField("WIN_BET", DoubleType(), True),
    StructField("OVER_2.5", BooleanType(), True),
    StructField("OVER_3.5", BooleanType(), True),
    StructField("H_15", BooleanType(), True),
    StructField("A_15", BooleanType(), True),
    StructField("H_45_50", BooleanType(), True),
    StructField("A_45_50", BooleanType(), True),
    StructField("H_90", BooleanType(), True),
    StructField("A_90", BooleanType(), True),
    StructField("H_Missing_Players", DoubleType(), True),
    StructField("A_Missing_Players", DoubleType(), True),
    StructField("Missing_Players", DoubleType(), True),
    StructField("H_Ball_Possession", StringType(), True),
    StructField("A_Ball_Possession", StringType(), True),
    StructField("H_Goal_Attempts", DoubleType(), True),
    StructField("A_Goal_Attempts", DoubleType(), True),
    StructField("H_Shots_on_Goal", DoubleType(), True),
    StructField("A_Shots_on_Goal", DoubleType(), True),
    StructField("H_Attacks", DoubleType(), True),
    StructField("A_Attacks", DoubleType(), True),
    StructField("H_Dangerous_Attacks", DoubleType(), True),
    StructField("A_Dangerous_Attacks", DoubleType(), True),
    StructField("H_Shots_off_Goal", DoubleType(), True),
    StructField("A_Shots_off_Goal", DoubleType(), True),
    StructField("H_Blocked_Shots", DoubleType(), True),
    StructField("A_Blocked_Shots", DoubleType(), True),
    StructField("H_Free_Kicks", DoubleType(), True),
    StructField("A_Free_Kicks", DoubleType(), True),
    StructField("H_Corner_Kicks", DoubleType(), True),
    StructField("A_Corner_Kicks", DoubleType(), True),
    StructField("H_Offsides", DoubleType(), True),
    StructField("A_Offsides", DoubleType(), True),
    StructField("H_Throw_in0", DoubleType(), True),
    StructField("A_Throw_in0", DoubleType(), True),
    StructField("H_Goalkeeper_Saves", DoubleType(), True),
    StructField("A_Goalkeeper_Saves", DoubleType(), True),
    StructField("H_Fouls", DoubleType(), True),
    StructField("A_Fouls", DoubleType(), True),
    StructField("H_Yellow_Cards0", DoubleType(), True),
    StructField("A_Yellow_Cards0", DoubleType(), True),
    StructField("Game Link", StringType(), True),
    StructField("match_result_category", StringType(), True)
])





def main():

    spark = SparkSession.builder \
        .appName("FootballMatchBatchJob") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "processed_football_events") \
        .load()


    df_parsed = df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")




    match_summary = df_parsed.select(
        col("Home").alias("home_team"),
        col("Away").alias("away_team"),
        col("League"),
        col("Date"),
        col("Time"),
        col("Round"),
        col("H_Score"),
        col("A_Score"),
        col("WIN"),
    
    ).withColumn(
        "total_match_goals",
        coalesce(col("H_Score"), lit(0)) + coalesce(col("A_Score"), lit(0))
    )





    print("\n__________________________ MATCH SUMMARY _____________________________________")
    match_summary.show(truncate=False)

   

    team_summary = df_parsed.select(
        col("Home").alias("team"), col("H_Score").alias("goals"),
        col("H_Shots_on_Goal").alias("shots_on_goal"),
        col("H_Fouls").alias("fouls"),
        col("H_Corner_Kicks").alias("corners")
    ).union(
        df_parsed.select(
            col("Away").alias("team"), col("A_Score").alias("goals"),
            col("A_Shots_on_Goal").alias("shots_on_goal"),
            col("A_Fouls").alias("fouls"),
            col("A_Corner_Kicks").alias("corners")
        )
    ).groupBy("team").agg(
        sum_("goals").alias("total_goals"),
        sum_("shots_on_goal").alias("total_shots_on_goal"),
        sum_("fouls").alias("total_fouls"),
        sum_("corners").alias("total_corners")
    ).withColumn(
    "team_class", 
    when(col("total_goals") > 10, "super team")
    .when(col("total_goals") > 5, "soccer team")
    .otherwise("average team")
).orderBy(col("total_goals").desc())


    print("\n________________________________ TEAM SUMMARY ___________________________________________")
    team_summary.show(truncate=False)

  

    incident_exploded = df_parsed.withColumn("incident", explode("INC"))


    incident_summary = incident_exploded.groupBy("Home", "Away", "Date", "Time").agg(
    count(col("incident.raw")).alias("num_incidents")
).withColumn(
    "time importance",
    when(col("num_incidents") > 5, "high importance")
    .when(col("num_incidents") > 2, "medium importance")
    .otherwise("low importance")
)

    print("\n__________________________ INCIDENTS (INC) _____________________________________")
    incident_summary.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()





# C:\kafka>.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
            
# PS C:\Users\Mina_> cd C:\kafka
# PS C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties



    # PS C:\kafka> .\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092   
    
    
    
# for spark Running  command 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 --master local[2] "C:\Users\Mina_\Desktop\python-BD\spark\main_spark_streaming.py"
