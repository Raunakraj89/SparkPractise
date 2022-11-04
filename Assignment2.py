import os
import sys
from datetime import datetime
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    sc=spark.sparkContext

# Read the input file
#department_file = sc.textFile(sys.argv[1])
#employee_file = sc.textFile(sys.argv[2])

    df = spark.read.csv(sys.argv[1], inferSchema=True,header=True)
    print("Department database schema")
    df.printSchema()

    ef = spark.read.csv(sys.argv[2], inferSchema=True,header=True)
    print("Employee database schema")
    ef.printSchema()



    df.createOrReplaceTempView("department")
    sql1DF = spark.sql("SELECT * FROM department")
    sql1DF.show()

    ef.createOrReplaceTempView("employee")
    sql1EF = spark.sql("SELECT * FROM employee")
    sql1EF.show()

    answer1 = spark.sql("SELECT SUM(SALARY) FROM employee WHERE DEPARTMENT_ID='20'")
    answer1.show()

    # answer2 = spark.sql("SELECT DEPARTMENT_ID,SUM(SALARY) FROM employee GROUP BY DEPARTMENT_ID ORDER BY SUM(SALARY)")
    # answer2.show()




    dt = datetime.now()
    output_bucket_path = (sys.argv[3]) + "output" + str(dt) + ".csv"
    #outDF = spark.createDataFrame(data=answer1, schema=["DEPARTMENT_ID", "SALARY"])
    answer1.write.csv(output_bucket_path)

