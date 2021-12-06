"""
This Python module contains an Apache Spark ETL job. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows:
    
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    jobs/etl_job.py

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

"""


import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id, \
    row_number, input_file_name, udf, substring
from pyspark.sql.window import Window


def startSpark(name="spark-etl", master='local[*]'):
    """Start Spark Sessions
    
    :param name: Spark job name
    :param master: Sets the Spark master URL to connect to
    :return: SparkSession object
    """
    spark = SparkSession.spark = (SparkSession
                                  .builder
                                  .master(master)
                                  .appName(name)
                                  .getOrCreate())
    return spark
    
    
def extractData(spark):
    """Load data from txt files
    
    :return: Spark DataFrame
    """
    dfRaw = spark.read.text("raw_logs/*/*.txt")
    return dfRaw


def getDate(string):
    """Get date from row
    
    :param string: row in string type
    :return: date in str type
    """
    timespamp = string[44:-1]
    date = datetime.datetime.fromtimestamp(int(timespamp)/1000.0)
    return str(date)


def getJobName(string):
    """Extract job name from row
    
    :param string: row in string type
    :return: date in str type
    """
    job = string.split('/')[6]
    return job


def transform(dfRaw):
    """Transform original dataset.
    
    :param dfRaw: Original Spark DataFrame
    :return: Transformed Spark DataFrame
    """
    dfTimeData = dfRaw.filter(dfRaw.value.startswith('{'))
    getDateUDF = udf(getDate, StringType())
    dfTimeData = dfTimeData.withColumn("datetimeUtc", 
            getDateUDF(dfTimeData["value"])).select("datetimeUtc")
    
    dfMetaData = dfRaw.filter(dfRaw.value.startswith('"'))
    splitCol = pyspark.sql.functions.split(dfMetaData.value, ' ')
    
    dfMetaData = (dfMetaData.withColumn("jobName",input_file_name())
                  .withColumn('topicName', splitCol.getItem(0))
                  .withColumn('topicPartition', splitCol.getItem(1))
                  .withColumn('currentOffset', splitCol.getItem(3)))
                  
    getJobNameUDF = udf(getJobName, StringType())
    getTopicNameUDF = udf(lambda x:x[1:-2])
    
    dfMetaData = (dfMetaData.withColumn("jobName",
                                getJobNameUDF(dfMetaData["jobName"]))
              .withColumn("topicName",
                          getTopicNameUDF(dfMetaData["topicName"]))
              .withColumn("topicPartition",
                          substring(dfMetaData["topicPartition"], 3, 2))
              .withColumn("currentOffset",
                          substring(dfMetaData["currentOffset"], 2, 3)))
    
    dfMetaData = dfMetaData.select("topicName",
                                   "topicPartition",
                                   "currentOffset")
    dfTimeData = (dfTimeData
                  .withColumn('row_index',
                              row_number()
                              .over(Window
                                    .orderBy(monotonically_increasing_id()))))
    
    dfMetaData = dfMetaData.withColumn('row_index',
    row_number().over(Window.orderBy(monotonically_increasing_id())))

    dfFinal = dfTimeData.join(dfMetaData,
                              on=["row_index"]).drop("row_index")
    
    return dfFinal


def load(dfTransformed, OutputFile):
    """Saves DataFrame into csv
    
    :param dfTransformed: Df to be saved
    :return: None
    """
    dfTransformed.toPandas().to_csv(OutputFile)
    
    
def main():
    """Main ETL script definition
    
    :return: None
    """
    spark = startSpark()
    dfRaw = extractData(spark)
    dfTransformed = transform(dfRaw)
    load(dfTransformed, 'output_file.csv')
    
if __name__ == '__main__':
    main()
