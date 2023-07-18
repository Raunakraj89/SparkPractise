from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# TODO(developer):
project_number = "maximal-helper-384300"
location = "us-east1-d"
subscription_id = "my-python-topic-sub"

spark = SparkSession.builder.appName("read-app").master("yarn").getOrCreate()

sdf = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        f"projects/{project_number}/locations/{location}/subscriptions/{subscription_id}",
    )
    .load()
)

sdf = sdf.withColumn("data", sdf.data.cast(StringType()))

query = (
    sdf.writeStream.format("console")
    .outputMode("append")
    .trigger(processingTime="1 second")
    .start()
)

# Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
query.awaitTermination(120)
query.stop()