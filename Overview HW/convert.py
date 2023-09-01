import pandas as pd
import subprocess
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Local CSV to Parquet Conversion").getOrCreate()

# List of CSV files, their local paths, and HDFS directories
files_to_convert = [
    ("train.csv", "C:/Users/Anastasiya_Viktarovi/Desktop/BigData/expedia-hotel-recommendations/train.csv",
     "/user/hive/warehouse/train"),
    ("test.csv", "C:/Users/Anastasiya_Viktarovi/Desktop/BigData/expedia-hotel-recommendations/test.csv",
     "/user/hive/warehouse/test"),
    ("destinations.csv", "C:/Users/Anastasiya_Viktarovi/Desktop/BigData/expedia-hotel-recommendations/destinations.csv",
     "/user/hive/warehouse/destinations"),
    ("sample_submission.csv",
     "C:/Users/Anastasiya_Viktarovi/Desktop/BigData/expedia-hotel-recommendations/sample_submission.csv",
     "/user/hive/warehouse/sample_submission")
]

# Define the batch size for data processing
batch_size = 10000

# Convert and save files using Pandas in batches
for csv_file, local_file, hdfs_dir in files_to_convert:
    chunks = pd.read_csv(local_file, chunksize=batch_size)
    for i, chunk in enumerate(chunks):
        # Process and save the chunk as Parquet locally
        parquet_file = local_file.replace(".csv", f".part_{i}.parquet")
        chunk.to_parquet(parquet_file, index=False)

        # Copy Parquet chunk to Cloudera container
        docker_parquet_file = f"/expedia-hotel-recommendations/{csv_file.replace('.csv', f'.part_{i}.parquet')}"
        docker_cp_command = f"docker cp {parquet_file} 73a1cbfe7dfd:{docker_parquet_file}"
        subprocess.call(docker_cp_command, shell=True)

        # Copy Parquet chunk from container to HDFS
        hdfs_put_command = \
            f"docker exec 73a1cbfe7dfd hdfs dfs -put" \
            f" {docker_parquet_file} {hdfs_dir}/{csv_file.replace('.csv', f'.part_{i}.parquet')}"
        subprocess.call(hdfs_put_command, shell=True)

# Stop the Spark session
spark.stop()
