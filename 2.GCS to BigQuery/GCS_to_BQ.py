# -*- coding: utf-8 -*-
"""
Created on Feb 18 16:15:00 2022

@author: Ujjwal Kumar
"""

import os
import logging
import argparse
import re
from datetime import datetime as dt

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

# Service Account Details (Update File Location)
Json_path = "C:/Users/SpringMl/Documents/1.Python Projects/Private Keys/pubsub-demo-341311-dataflow.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Json_path

# Update Details if Required
#Project Detail
PROJECT_ID = 'pubsub-demo-341311'

#BigQuery Details
DATASET = 'Test'
TABLE = 'Bike_Data'

#DataFlow Details
JOB ='filetobq'
REGION = 'us-east1'

#GCS Details
DATA_PATH = 'gs://pubsub-demo-341311/bike-sample.csv'
TEMP_LOCATION = 'gs://pubsub-demo-341311/temp'
STAG_LOCATION = 'gs://pubsub-demo-341311/stag'


def deconcat(element):
    if ',' not in element[:10]:
        if element[0]=="9":
            element = element[:7]+','+element[7:]
        elif element[0]=="1":
            element = element[:8]+','+element[8:]
    return element

def replace_nulls(element):
    return element.replace('NULL','')

def format_datetime_bq(element):
    ref = element.find('/20')
    start = element[:ref].rfind(',')+1
    end = ref+element[ref:].find(',')
    element = element[:start]+dt.strftime(dt.strptime(element[start:end], '%m/%d/%Y %H:%M'),
                                          '%Y-%m-%d %H:%M:%S')+element[end:]

    rref = element.rfind('/20')
    start = element[:rref].rfind(',')+1
    end = rref+element[rref:].find(',')
    element = element[:start]+dt.strftime(dt.strptime(element[start:end],'%m/%d/%Y %H:%M'),
                                          '%Y-%m-%d %H:%M:%S')+element[end:]

    return element

def parse_method(string_input):
    """This method translates a single line of comma separated values to a dictionary 
    which can be loaded into BigQuery.
    Args:
    string_input: A comma separated list of values in the form: 'Trip_Id, Trip__Duration,
    Start_Station_Id, Start_Time, Start_Station_Name, End_Station_Id, End_Time, 
    End_Station_Name, Bike_Id, User_Type'
    Returns:
    A dict mapping BigQuery column names as keys to the corresponding value
    parsed from string_input.
    """
    # Strip out carriage return, newline and quote characters.
    values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
    row = dict(
        zip(('Trip_Id', 'Trip__Duration', 'Start_Station_Id', 'Start_Time',
        'Start_Station_Name', 'End_Station_Id', 'End_Time', 'End_Station_Name',
        'Bike_Id', 'User_Type'),values))
    return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Parse arguments from the command line.
    ns,beam_args = parser.parse_known_args(argv)

    # Setting Pipeline Options
    beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project= PROJECT_ID,
    job_name=JOB,
    temp_location=TEMP_LOCATION,
    staging_location = STAG_LOCATION,
    region=REGION)

    # Initiate the pipeline using the pipeline Options

    p = beam.Pipeline(options= beam_options)

    (p  | 'Read File' >> beam.io.ReadFromText(DATA_PATH, skip_header_lines=1)
        | 'Deconcactenate Columns' >> beam.Map(deconcat)
        | 'Replace Nulls' >> beam.Map(replace_nulls)
        | 'Convert to BQ Datetime' >> beam.Map(format_datetime_bq)
        | 'String To BigQuery Row' >> beam.Map(parse_method)
        | 'Write To BigQuery' >> beam.io.WriteToBigQuery(TABLE,DATASET,PROJECT_ID,
    schema='Trip_Id:INTEGER,Trip__Duration:INTEGER,Start_Station_Id:INTEGER,'
    'Start_Time:DATETIME,Start_Station_Name:STRING,End_Station_Id:INTEGER,'
    'End_Time:DATETIME, End_Station_Name:STRING, Bike_Id:INTEGER, User_Type:STRING',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    p.run().wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()