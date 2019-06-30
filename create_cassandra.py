# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster


def create_and_set_keyspace():
    try:
        cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
        session = cluster.connect()
    except Exception as e:
        print(e)  
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS udacity 
            WITH REPLICATION = 
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )
    except Exception as e:
        print(e)
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)

def main():
   create_and_set_keyspace() 

if __name__ == "__main__":
    main()