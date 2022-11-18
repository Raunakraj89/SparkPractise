import os
import sys
from datetime import datetime
from operator import add
import argparse

from pyspark.sql import SparkSession

# Initialize parser
    parser = argparse.ArgumentParser(description='wordcount project')

# Adding optional argument
    parser.add_argument("--arg1", help="input file path")
    parser.add_argument("--arg2", help="output file path")
    #
    # Read arguments from command line
    args = parser.parse_args()





if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    sc=spark.sparkContext

# Read the input file and Calculating words count
text_file = args.arg1
#text_file = sc.textFile(sys.argv[1])
counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)

# Printing each word with its respective count
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

#df=output.toDf()
#output.write.csv('/tmp/mycsv.csv')
#df.coalesce(1).write.csv('mycsv.csv')
#gsutil cp /tmp/mycsv.csv gs://output_will_go_here/

dt = datetime.now()
output_bucket_dir = args.arg2
output_bucket_path = output_bucket_dir + "output" + str(dt) + ".csv"
output_bucket_path = (sys.argv[2]) + "output" + str(dt) + ".csv"
outDF = spark.createDataFrame(data=output, schema=["Word", "Count"])
outDF.write.csv(output_bucket_path)

#os.system('gsutil cp /tmp/output2.csv gs://output_will_go_here/')

#gsutil cp -r "/tmp/mycsv.csv" "gs://output_will_go_here/"

df = text_file.withColumn('words', split(col('value'), ' ')) \
    .withColumn('word', explode(col('words'))) \
    .drop('value', 'words').groupBy('word').agg(count('word') \
    .alias('count')).orderBy('count', ascending=False)
df.show()
df.write.csv(output_bucket_path)
