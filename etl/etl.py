# -*- coding: utf-8 -*-
"""
Spark ETL script
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
    
    
def extract():
    """
    open file
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
    """
    main function with all transformations.
    because spark processing every line in file as separate
    row, it starting working with 2 DFs.
    then, it merging them
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


def load(dfFinal):
    '''
    saves DataFrame into csv tabular file format file
    '''
    dfFinal.toPandas().to_csv('output_file.csv')
    
    
def main():
    dfRaw = extract()
    dfFinal = transform(dfRaw)
    load(dfFinal)
    
if __name__ == '__main__':
    main()