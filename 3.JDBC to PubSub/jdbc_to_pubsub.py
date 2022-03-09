# -*- coding: utf-8 -*-
"""
Created on Tue Feb 22 23:15:00 2022

@author: Ujjwal Kumar
"""

import argparse
from sys import argv
from encodings import utf_8
import os
import json
import typing

import unicodedata

from sqlalchemy import Unicode

from apache_beam.coders import RowCoder,registry

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
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
    job_name='jdbctops',
    temp_location='gs://pubsub-demo-341311/temp',
    region='us-east1')


ExampleRow = typing.NamedTuple('ExampleRow',[('product_id', Unicode), ('product_name', Unicode) , ('brand' ,Unicode)])
registry.register_coder(ExampleRow, RowCoder)


#Creating Pipeline
p = beam.Pipeline(options=beam_options)

# json.dumps with indent=2).encode('utf-8')) is used to converd Dictionary data to Byte String to send it to Pubsub 
(p | 'ReadData' >> ReadFromJdbc(table_name = 'products',
                                            driver_class_name='org.postgresql.Driver',
                                            jdbc_url='jdbc:postgresql://localhost:5432/Product-Management',
                                            username='postgres',
                                            password='mysecretpassword',
                                            query = 'SELECT product_id, product_name, brand FROM products;'
                                            )
       | 'PrintData' >> beam.Map(print)
)

p.run()
