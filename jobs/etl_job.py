"""
This Python module contains an Apache Spark ETL job. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows:
    
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py
    
where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

"""


import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id, \
    row_number, input_file_name, udf
from pyspark.sql.window import Window


spark = SparkSession.spark = SparkSession.builder \
    .master("local") \
    .appName("spark-etl") \
    .getOrCreate()
    
    
def extractData():
    """Load data from txt files
    
    :return: Spark DataFrame
    """
    dfRaw = spark.read.text("raw_logs/*/*.txt")
    return dfRaw


def getDate(string):
    timespamp = string[44:-1]
    date = datetime.datetime.fromtimestamp(int(timespamp)/1000.0)
    return str(date)


def getJobName(string):
    job = string.split('/')[6]
    return job


def getTopicName(string):
    topic = string[1:-2]
    return topic


def getTopicPartition(string):
    partition = string[2:-1]
    return partition


def getCurrentOffset(string):
    offset = string[1:-2]
    return offset


def transform(dfRaw):
    """Transform original dataset.
    
    :param dfRaw: Original Spark DataFrame
    :return: Transformed Spark DataFrame
    """
    dfTime = dfRaw.filter(dfRaw.value.startswith('{'))
    getDateUDF = udf(getDate, StringType())
    dfTime = dfTime.withColumn("datetimeUtc", 
            getDateUDF(dfTime["value"])).select("datetimeUtc")
    
    dfMeta = dfRaw.filter(dfRaw.value.startswith('"'))
    splitCol = pyspark.sql.functions.split(dfMeta.value, ' ')
    
    dfMeta = dfMeta.withColumn("jobName",
                               input_file_name())
    dfMeta = dfMeta.withColumn('topicName',
                               splitCol.getItem(0))
    dfMeta = dfMeta.withColumn('topicPartition',
                               splitCol.getItem(1))
    dfMeta = dfMeta.withColumn('currentOffset',
                               splitCol.getItem(3))
    dfMeta = dfMeta.select("jobName", "topicName",
                           "topicPartition", "currentOffset")
    
    getJobNameUDF = udf(getJobName, StringType())
    getTopicNameUDF = udf(getTopicName, StringType())
    getTopicPartitionUDF = udf(getTopicPartition, StringType())
    getCurrentOffsetUDF = udf(getCurrentOffset, StringType())
    
    dfMeta=dfMeta.withColumn("jobName",
                             getJobNameUDF(dfMeta["jobName"]))
    dfMeta=dfMeta.withColumn("topicName",
                             getTopicNameUDF(dfMeta["topicName"]))
    dfMeta=dfMeta.withColumn("topicPartition",
                    getTopicPartitionUDF(dfMeta["topicPartition"]))
    dfMeta=dfMeta.withColumn("currentOffset",
                    getCurrentOffsetUDF(dfMeta["currentOffset"]))
    
    dfTime=dfTime.withColumn('row_index',
    row_number().over(Window.orderBy(monotonically_increasing_id())))
    
    dfMeta=dfMeta.withColumn('row_index',
    row_number().over(Window.orderBy(monotonically_increasing_id())))

    dfFinal = dfTime.join(dfMeta,
                          on=["row_index"]).drop("row_index")
    
    return dfFinal


def load(dfTransformed):
    """Saves DataFrame into csv
    
    :param dfTransformed: Df to be saved
    :return: None
    """
    dfTransformed.toPandas().to_csv('output_file.csv')
    
    
def main():
    """Main ETL script definition
    
    :return: None
    """
    dfRaw = extractData()
    dfTransformed = transform(dfRaw)
    load(dfTransformed)
    
if __name__ == '__main__':
    main()