# -*- coding: utf-8 -*-
"""
Created on March 09 2022

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
DATASET = 'Dataflow'
TABLE = 'School_data'

#DataFlow Details
JOB ='filestobq'
REGION = 'us-east1'

#GCS Details
#Data Path
DATA_PATH_ELEC = 'gs://pubsub-demo-341311/Schools/percentage-of-schools-with-electricity-2013-2016.csv'
DATA_PATH_WATER = 'gs://pubsub-demo-341311/Schools/percentage-of-schools-with-water-facility-2013-2016.csv'
DATA_PATH_ENROLL = 'gs://pubsub-demo-341311/Schools/gross-enrollment-ratio-2013-2016.csv'
DATA_PATH_COMP =  'gs://pubsub-demo-341311/Schools/percentage-of-schools-with-comps-2013-2016.csv'
#Pipeline Files
TEMP_LOCATION = 'gs://pubsub-demo-341311/temp'
STAG_LOCATION = 'gs://pubsub-demo-341311/stag'

# Custom ParDo Class  < Not Used Here>
class Formatting(beam.DoFn):
    def process(self,element):
        cur = element.split(',')
        v_key = cur[0]+"|"+cur[1]
        v_value = cur[2]+"|"+cur[3]+"|"+cur[4]+"|"+cur[5]

        return (v_key,v_value)

# Custom Map Function 
def formatting_KV(data):
    cur = data.split(',')
    v_key = cur[0]+"|"+cur[1]
    v_value = cur[2]+"|"+cur[3]+"|"+cur[4]+"|"+cur[5]+"|"+cur[6]

    return (v_key,v_value)

def parse_row(KV_input):
    values = KV_input[0].split('|')
    try:
        values.append(KV_input[1]['electricity'][0].split('|')[4])
    except:
        values.append(0)
    try:
        values.append(KV_input[1]['water'][0].split('|')[4])
    except:
        values.append(0)
    try:
        values.append(KV_input[1]['computer'][0].split('|')[4])
    except:
        values.append(0)
    try:
        values.append(KV_input[1]['Enrollment'][0].split('|')[2])
    except:
        values.append(0)

    row = dict(
        zip(('Region', 'Year', 'Electricity', 'Water',
        'Computer', 'Primary_Gross_Enrollment_Ratio'),values))
    return row




def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Parse arguments from the command line.
    ns,beam_args = parser.parse_known_args(argv)

    # Setting Pipeline Options
    beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',  #'DirectRunner',
    project= PROJECT_ID,
    job_name=JOB,
    temp_location=TEMP_LOCATION,
    staging_location = STAG_LOCATION,
    region=REGION)

    # Initiate the pipeline using the pipeline Options
    with beam.Pipeline(options= beam_options) as pipeline:
        with_electricity = ( pipeline 
                                | 'Read Electricity File' >> beam.io.ReadFromText(DATA_PATH_ELEC, skip_header_lines=1) 
                                | 'Format Input ELEC' >> beam.Map(formatting_KV) 
                            )
        with_water = ( pipeline 
                        | 'Read Water Supply File' >> beam.io.ReadFromText(DATA_PATH_WATER, skip_header_lines=1) 
                        | 'Format Input WATER' >> beam.Map(formatting_KV)
                    )
        with_computer = ( pipeline 
                        | 'Read Computer File' >> beam.io.ReadFromText(DATA_PATH_COMP, skip_header_lines=1) 
                        | 'Format Input Computer' >> beam.Map(formatting_KV)
                    )
        Enrollment = ( pipeline 
                        | 'Read Enrollment File' >> beam.io.ReadFromText(DATA_PATH_ENROLL, skip_header_lines=1) 
                        | 'Format Input Enrollment' >> beam.Map(formatting_KV)
                    )

        Output = (({'electricity': with_electricity, 'water': with_water,'computer':with_computer,'Enrollment':Enrollment})
                | 'Merge' >> beam.CoGroupByKey()
                | 'Formatting to BQ Row' >>beam.Map(parse_row)
                #| 'Output' >> beam.Map(print)
                | 'Write To BigQuery' >> beam.io.WriteToBigQuery(TABLE,DATASET,PROJECT_ID,
    schema='Region:STRING,Year:STRING,Electricity:STRING,Water:STRING,Computer:STRING,Primary_Gross_Enrollment_Ratio:STRING',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()