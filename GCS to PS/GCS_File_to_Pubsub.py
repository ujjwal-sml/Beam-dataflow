# -*- coding: utf-8 -*-
"""
Created on Sun Feb 15 23:15:00 2022

@author: Ujjwal Kumar
"""

import argparse
from sys import argv
from encodings import utf_8
import os
import json 

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1

# Service Account Details (Update File Location)
Json_path = "C:/Users/SpringMl/Documents/1.Python Projects/Private Keys/pubsub-demo-341311-dataflow.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Json_path


parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()

# Setting Pipeline Options
# Update Details if Required
beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='pubsub-demo-341311',
    job_name='filetops',
    temp_location='gs://pubsub-demo-341311/temp',
    region='us-east1')

#p1 = p | 'Match File' >>beam.io.filesystem.MatchResult('gs://ag-pipeline/batch/*')

#Creating Pipeline
p = beam.Pipeline(options=beam_options)

# json.dumps with indent=2).encode('utf-8')) is used to converd Dictionary data to Byte String to send it to Pubsub 
(p | 'ReadData' >> beam.io.ReadFromText('gs://pubsub-demo-341311/beers.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: json.dumps({"sr": x[0], "abv": x[1], "ibu": x[2], "id": x[3], "name": x[4], "style": x[5], "brewery_id": x[6], "ounces": x[7]},indent=2).encode('utf-8'))
       | 'WriteToPubSub' >> beam.io.gcp.pubsub.WriteToPubSub('projects/pubsub-demo-341311/topics/User-add'))

p.run()
