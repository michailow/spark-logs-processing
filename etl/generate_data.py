# -*- coding: utf-8 -*-

"""
Generating data
"""
from generator import generator

def main():
    NUMBER_OF_JOBS = 21
    MAX_PARTITIONS = 51
    generator(NUMBER_OF_JOBS, MAX_PARTITIONS)
    input()
    
if __name__ == "__main__":
    main()