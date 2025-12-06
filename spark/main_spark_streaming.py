from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as sum_, count, explode
from pyspark.sql.types import *
from pyspark.sql.functions import lit, coalesce, expr, when

# Define the event schema
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

def write_to_mysql(df, batch_id, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/Big_Data_DB") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "Gtrs3695$") \
        .mode("append") \
        .save()

def main():

    import sys
    import io
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    

    spark = SparkSession.builder \
        .appName("FootballMatchBatchJob") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n==> Reading data from Kafka topic 'processed_football_events'...\n")

    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "processed_football_events") \
        .option("startingOffsets", "earliest") \
        .load()
    

    array_schema = ArrayType(event_schema)
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), array_schema).alias("data_array")
    ).select(
        explode(col("data_array")).alias("data")
    ).select("data.*")
    
    # Match Summary
    match_summary = df_parsed.select(
        col("Home").alias("home_team"),
        col("Away").alias("away_team"),
        col("League"),
        col("Date"),
        col("Time"),
        col("Round"),
        col("H_Score"),
        col("A_Score"),
        col("WIN")
    ).withColumn(
        "total_match_goals",
        col("H_Score") +col("A_Score"))

    # Team Summary
    team_summary = df_parsed.select(
            col("Home").alias("team"),
            col("H_Score").alias("goals"),
            col("H_Shots_on_Goal").alias("shots_on_goal"),
            col("H_Fouls").alias("fouls"),
            col("H_Corner_Kicks").alias("corners")
        ).union(
            df_parsed.select(
                col("Away").alias("team"),
                col("A_Score").alias("goals"),
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
            .otherwise("average team"))
    
 
    incident_exploded = df_parsed.filter(col("INC").isNotNull()).withColumn("incident", explode("INC"))
    
    incident_summary = incident_exploded.groupBy("Home", "Away", "Date", "Time").agg(
        count(col("incident.raw")).alias("num_incidents")
    ).withColumn(
        "time_importance",
        when(col("num_incidents") > 5, "high importance")
        .when(col("num_incidents") > 2, "medium importance")
        .otherwise("low importance")
    )

    # Display results in console
  
    # match_summary.writeStream \
    # .outputMode("append") \
    # .format("console") \
    # .option("truncate", False) \
    # .start()
    
    # team_summary.writeStream \
    # .outputMode("complete") \
    # .format("console") \
    # .option("truncate", False) \
    # .start()
    
    
    # incident_summary.writeStream \
    # .outputMode("complete") \
    # .format("console") \
    # .option("truncate", False) \
    # .start()
    #send result to DB
    match_summary.writeStream \
        .foreachBatch(lambda df, batch_id: write_to_mysql(df, batch_id, "match_summary")) \
        .outputMode("append") \
        .start()

    team_summary.writeStream \
        .foreachBatch(lambda df, batch_id: write_to_mysql(df, batch_id, "team_summary")) \
        .outputMode("complete") \
        .start()

    incident_summary.writeStream \
        .foreachBatch(lambda df, batch_id: write_to_mysql(df, batch_id, "incident_summary")) \
        .outputMode("complete") \
        .start()
    spark.streams.awaitAnyTermination()
 

    spark.stop()


if __name__ == "__main__":
    main()

