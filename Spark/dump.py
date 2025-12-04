from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "processed_football_events"

# Define schema - MATCHES YOUR PRODUCER
event_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("team", StringType(), True),
    StructField("player", StringType(), True),
    StructField("goals", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("yellow_cards", IntegerType(), True),
    StructField("red_cards", IntegerType(), True),
    StructField("home_away", StringType(), True)
])

def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("FootballEventStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def write_to_mysql(df, table_name):
    """Write DataFrame to MySQL - using overwrite to avoid duplicates"""
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3307/Big_Data_DB") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "Gtrs3695$") \
        .mode("overwrite") \
        .save()

def main():
    """Main execution function"""
    try:
        # Initialize Spark
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        
        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("Connected to Kafka successfully")
        
        # Parse JSON
        df_parsed = df.selectExpr("CAST(value AS STRING) AS json_str") \
            .select(from_json(col("json_str"), event_schema).alias("data")) \
            .select("data.*")
        
        # Player statistics - goals and assists
        player_stats = df_parsed.groupBy("player", "team", "home_away", "match_id") \
            .agg(
                expr("sum(goals)").alias("total_goals"),
                expr("sum(assists)").alias("total_assists"),
                expr("sum(yellow_cards)").alias("total_yellow_cards"),
                expr("sum(red_cards)").alias("total_red_cards"),
                expr("count(*)").alias("matches_played")
            )
        
        logger.info("=" * 70)
        logger.info("FOOTBALL EVENT STREAMING - STARTED")
        logger.info("=" * 70)
        logger.info("Waiting for events from Kafka topic: processed_football_events")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        
        # Write to memory table
        query = player_stats.writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("player_stats") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "D:/Bigdata-project/checkpoints/player_stats") \
            .start()
        
        logger.info("Stream started - Writing to in-memory table 'player_stats'")
        
        # Query and display results periodically
        import time
        while query.isActive:
            time.sleep(5)
            
            # Player statistics
            result_df = spark.sql("""
                SELECT 
                    player,
                    team,
                    home_away,
                    match_id,
                    total_goals,
                    total_assists,
                    total_yellow_cards,
                    total_red_cards,
                    matches_played
                FROM player_stats
                ORDER BY total_goals DESC, total_assists DESC
            """)
            
            if result_df.count() > 0:
                logger.info("\n" + "=" * 70)
                logger.info("CURRENT PLAYER STATISTICS:")
                logger.info("=" * 70)
                result_df.show(20, truncate=False)
                
                # Write player stats to MySQL database
                try:
                    write_to_mysql(result_df, "player_statistics")
                    logger.info("✓ Player statistics written to MySQL successfully")
                except Exception as e:
                    logger.error(f"✗ Error writing player statistics to MySQL: {e}")
                
                # TEAM SUMMARY
                team_df = spark.sql("""
                    SELECT 
                        team,
                        home_away,
                        SUM(total_goals) AS team_goals,
                        SUM(total_assists) AS team_assists,
                        SUM(total_yellow_cards) AS team_yellows,
                        SUM(total_red_cards) AS team_reds
                    FROM player_stats
                    GROUP BY team, home_away
                    ORDER BY team_goals DESC
                """)
                
                logger.info("\n" + "=" * 70)
                logger.info("TEAM SUMMARY:")
                logger.info("=" * 70)
                team_df.show(10, truncate=False)
                
                try:
                    write_to_mysql(team_df, "team_summary")
                    logger.info("✓ Team summary written to MySQL successfully")
                except Exception as e:
                    logger.error(f"✗ Error writing team summary to MySQL: {e}")
                # MATCH SUMMARY
                match_df = spark.sql("""
                    SELECT
                        match_id,
                        SUM(total_goals) AS goals_in_match,
                        SUM(total_assists) AS assists_in_match,
                        SUM(total_yellow_cards) AS yellows_in_match,
                        SUM(total_red_cards) AS reds_in_match
                    FROM player_stats
                    GROUP BY match_id
                    ORDER BY goals_in_match DESC
                """)
                try:
                    write_to_mysql(match_df, "match_summary")
                    logger.info("✓ Match summary written to MySQL successfully")
                except Exception as e:
                    logger.error(f"✗ Error writing match summary to MySQL: {e}")
                logger.info("\n" + "=" * 70)
                logger.info("MATCH SUMMARY:")
                logger.info("=" * 70)
                match_df.show(10, truncate=False)
            else:
                logger.info("Waiting for data... (Make sure producer is sending events)")
        
    except KeyboardInterrupt:
        logger.info("\nStopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Application error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Shutting down gracefully...")
        if 'spark' in locals():
            spark.stop()

# For Spark running command:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 --jars D:\path\to\mysql-connector-java-8.0.33.jar --master local[2] "C:\Users\Mina_\Desktop\python-BD\spark\main_spark_streaming.py"

if __name__ == "__main__":
    main()