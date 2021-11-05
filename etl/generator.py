# -*- coding: utf-8 -*-
"""
Module to generate some logs for the task
To create ETL scripts abd test it we will need some data
For that purpose I created this genarator
Which will help me get the generated data
"""


import os
import random


FOLDERNAME = 'raw_logs'

def createJobsFolder(FOLDERNAME):
    """
    create folders for the logs if not exits
    then it return a path of that folder
    """
    currentDirectory = os.getcwd()
    finalDirectory = os.path.join(currentDirectory, FOLDERNAME)
    if not os.path.exists(finalDirectory):
        os.makedirs(finalDirectory)
    return finalDirectory


def checkLastJob(jobsFolder):
    """
    in case there are already folders with logs
    count number of the jobs folders
    """
    allFolders = os.listdir(jobsFolder)
    jobsFolders = [f for f in allFolders if f.startswith('job')]
    jobsCount = len(jobsFolders)
    return jobsCount


def createFolder(jobsFolder, jobNumber):
    """
    creating folders for the jobs
    """
    folderName = "job" + str(jobNumber)
    directory = os.path.join(jobsFolder, folderName)
    os.makedirs(directory)
    return directory


batchWatermarkMs = 0
METADATA_START = 1626616200136
METADATA_END = 1636042843000

def generateMetaData():
    """
    genetaring random metadata for logs
    """
    batchTimestampMs = random.randrange(METADATA_START,
                                        METADATA_END)
    metaData = str({"batchWatermarkMs":batchWatermarkMs,
                    "batchTimestampMs":batchTimestampMs})
    return metaData


MAX_OFFSET = 200

def generateCommitInfo(topic, partition):
    """
    creating commit info layer for logs file
    """
    offset = random.choice(range(1, MAX_OFFSET))
    commit = '"{topic}": {{"{partition}" :'\
            ' "{offset}"}}'.format(topic=topic,
                                   partition=partition,
                                   offset=offset)
    return commit
    

topicsFile = 'topics'

def openTopics():
    """
    opens topics file
    """
    
    with open(topicsFile) as f:
        topics = f.read().split()
    return topics


def generateLogs(currentFolder, MAX_PARTITIONS):
    """
    function open a topics file to get list of all topics
    then for each topic it creating random number of partions
    for each partition functions creating file
    and filling then with random-generater data
    in accordance with the requirements
    """
    topics = openTopics()
    for topic in topics:
        metaData = generateMetaData()
        numberOfPartitions = random.choice(range(1, MAX_PARTITIONS))
        for partition in range(1, numberOfPartitions):
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
    """
    jobsFolder = createJobsFolder(FOLDERNAME)
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