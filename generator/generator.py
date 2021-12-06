# -*- coding: utf-8 -*-
"""
Module to generate some logs for the task
To create ETL scripts abd test it we will need some data
For that purpose I created this genarator
Which will help me get the generated data
"""


import os
import random


def createJobsFolder():
    """Create folders for the logs if not exits
    
    :return: directory for logs
    """
    FOLDERNAME = 'raw_logs'
    currentDirectory = os.getcwd()
    finalDirectory = os.path.join(currentDirectory, FOLDERNAME)
    if not os.path.exists(finalDirectory):
        os.makedirs(finalDirectory)
    return finalDirectory


def checkLastJob(jobsFolder):
    """Count number of folders in folder
    
    :param jobsFolder: directory with jobs
    :return: number of created jobs
    """
    allFolders = os.listdir(jobsFolder)
    jobsFolders = [f for f in allFolders if f.startswith('job')]
    jobsCount = len(jobsFolders)
    return jobsCount


def createFolder(jobsFolder, jobNumber):
    """Creating folders for the jobs
    
    :param jobsFolder: directory with jobs
    :param jobNumber: number of exiting jobs
    :return: directory with job folder
    """
    folderName = "job" + str(jobNumber)
    directory = os.path.join(jobsFolder, folderName)
    os.makedirs(directory)
    return directory


def generateMetaData():
    """Genetaring random metadata for logs
    
    :return: string with logs metadata
    """
    batchWatermarkMs = 0
    METADATA_START = 1626616200136
    METADATA_END = 1636042843000
    batchTimestampMs = random.randrange(METADATA_START,
                                        METADATA_END)
    metaData = str({"batchWatermarkMs":batchWatermarkMs,
                    "batchTimestampMs":batchTimestampMs})
    return metaData


def generateCommitInfo(topic, partition):
    """Creating commit info layer for logs file
    
    :param topic: log topic name
    :param partition: log partition number
    :return: data for logs
    """
    MAX_OFFSET = 200
    offset = random.choice(range(100, MAX_OFFSET))
    commit = '"{topic}": {{"{partition}" :'\
            ' "{offset}"}}'.format(topic=topic,
                                   partition=partition,
                                   offset=offset)
    return commit
    

def openTopics():
    """Opens topics file
    
    :return: list of topics
    """
    topicsFile = 'topics'
    with open(topicsFile) as f:
        topics = f.read().split()
    return topics


def generateLogs(currentFolder, MAX_PARTITIONS):
    """Function open a topics file to get list of all topics
    then for each topic it creating random number of partions
    for each partition functions creating file
    and filling then with random-generater data
    in accordance with the requirements
    
    :param currentFolder: job folder
    :param MAX_PARTITIONS: number of max partitions
    :return: None
    """
    topics = openTopics()
    for topic in topics:
        metaData = generateMetaData()
        numberOfPartitions = random.choice(range(1, MAX_PARTITIONS))
        for partition in range(10, numberOfPartitions):
            logName = "log{topic}" \
                "{partition}.txt".format(topic=topic,
                                         partition=partition)
            completeName = os.path.join(currentFolder, logName)
            file = open(completeName, "w")
            file.write('v1')
            file.write('\n')
            file.write(metaData)
            file.write('\n')
            commitInfo = generateCommitInfo(topic, partition)
            file.write(commitInfo)
            file.write('\n')
            file.close()


def generator(NUMBER_OF_JOBS, MAX_PARTITIONS):
    """
    function generating folders for jobs
    and then filling them with generated data
    
    :param NUMBER_OF_JOBS: number of jobs to create
    :param MAX_PARTITIONS: number of max partitions
    :return: None
    """
    jobsFolder = createJobsFolder()
    lastJob = checkLastJob(jobsFolder)
    
    for nJobs in range(1, NUMBER_OF_JOBS):
        currentFolder = createFolder(jobsFolder, lastJob + nJobs)
        generateLogs(currentFolder, MAX_PARTITIONS)
    print("Data created")
        

def main():
    greetingMessage = ["Predifined values:",
                       "Number of jobs: 15",
                       "Number of Partitions: 50",
                       "Press Enter to select standart values"]
    print(*greetingMessage, sep='\n')
    while True:
        try:
            NUMBER_OF_JOBS = input("Write number of jobs: ")
            MAX_PARTITIONS = input("Write number of partitions: ")
            generator(int(NUMBER_OF_JOBS + 1),
                      int(MAX_PARTITIONS) + 1)
        except Exception as e:
            print("Functions accepts numbers")
            print(e)
            
if __name__ == "__main__":
    main()