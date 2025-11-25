import os
import urllib.request

# List of JAR files to download
jars = {
    "spark-sql-kafka-0-10_2.12-3.2.4.jar": "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.4/spark-sql-kafka-0-10_2.12-3.2.4.jar",
    "spark-streaming-kafka-0-10-assembly_2.12-3.2.4.jar": "https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.2.4/spark-streaming-kafka-0-10-assembly_2.12-3.2.4.jar",
    "kafka-clients-3.9.1.jar": "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar",
    "commons-pool2-2.11.1.jar": "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
}

# Download each JAR
for jar_name, url in jars.items():
    if not os.path.exists(jar_name):
        print(f"Downloading {jar_name} ...")
        try:
            urllib.request.urlretrieve(url, jar_name)
            print(f"{jar_name} downloaded successfully.")
        except Exception as e:
            print(f"Failed to download {jar_name}: {e}")
    else:
        print(f"{jar_name} already exists. Skipping download.")

print("\nAll JARs are ready. Place this Python file in the folder with your Spark script.")
