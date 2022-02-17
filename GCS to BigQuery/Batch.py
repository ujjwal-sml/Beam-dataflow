# -*- coding: utf-8 -*-
"""
Created on Sun Feb 13 16:15:00 2022

@author: SpringMl
"""

import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import os

# Service Account Details (Update File Location)
Json_path = "C:/Users/SpringMl/Documents/1.Python Projects/Private Keys/pubsub-demo-341311-dataflow.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Json_path

# Update Details if Required
PROJECT_ID = 'pubsub-demo-341311'

SCHEMA = 'sr:INTEGER,abv:FLOAT,id:INTEGER,name:STRING,style:STRING,ounces:FLOAT'
DATASET = 'Test'
TABLE = 'Kaggle_Data'

JOB ='filetoBQ'
REGION = 'us-east1'

DATA_PATH = 'gs://pubsub-demo-341311/beers.csv'
TEMP_LOCATION = 'gs://pubsub-demo-341311/temp'
STAG_LOCATION = 'gs://pubsub-demo-341311/stag'
TEMP_LOCATION1 = 'gs://pubsub-demo-341311/temp1'


def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['abv']) > 0 and len(data['id']) > 0 and len(data['name']) > 0 and len(data['style']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    data['abv'] = float(data['abv']) if 'abv' in data else None
    data['id'] = int(data['id']) if 'id' in data else None
    data['name'] = str(data['name']) if 'name' in data else None
    data['style'] = str(data['style']) if 'style' in data else None
    data['ounces'] = float(data['ounces']) if 'ounces' in data else None
    return data

def del_unwanted_cols(data):
    """Delete the unwanted columns"""
    del data['ibu']
    del data['brewery_id']
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    nm ,beam_args = parser.parse_known_args(argv)

    # Setting Pipeline Options
    beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project= PROJECT_ID,
    job_name=JOB,
    temp_location=TEMP_LOCATION,
    staging_location = STAG_LOCATION,
    region=REGION)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText(DATA_PATH, skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"sr": x[0], "abv": x[1], "ibu": x[2], "id": x[3], "name": x[4], "style": x[5], "brewery_id": x[6], "ounces": x[7]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(TABLE,
            DATASET,
            PROJECT_ID,
           schema=SCHEMA,
           method='STREAMING_INSERTS',
           create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
           ))
    result = p.run()
    result.wait_until_finish()