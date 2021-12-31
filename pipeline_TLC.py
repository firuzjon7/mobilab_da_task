import logging
import pandas as pd
from datetime import datetime
import urllib.request as urllib2
from urllib.error import HTTPError, URLError
import urllib.parse as urlparse
import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.transforms.core import FlatMap, Map, ParDo
from apache_beam.io import fileio
from apache_beam.io import filesystems
from apache_beam.options.pipeline_options import PipelineOptions
import os
from os import listdir
from os.path import isfile, join
from socket import timeout
import glob
import pyarrow


def reading_files_from_url(data, *args):
    """creating function to read all urls and make a list with possible url for extracting files"""
    print(" -- Downloading Files for -- {}".format(data))
    if data == 'Yellow':
        input_list = list()
        input_list_1 = list()
        input_list_2 = list()
        for year in args:
            if year == 2021:  # if the last year is given, the datasts only for the last 6 month will be extracted
                input = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-0'
                for month in range(1, 7):
                    input_list.append(input + str(month) + '.csv')
            elif year == 2020:
                input = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-0'
                for month in range(1, 10):
                    input_list_1.append(input + str(month) + '.csv')
                input_10 = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-'
                for month in range(10, 13):
                    input_list_1.append(input_10 + str(month) + '.csv')
            elif year == 2019:
                input = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-0'
                for month in range(1, 10):
                    input_list_2.append(input + str(month) + '.csv')
                input_10 = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-'
                for month in range(10, 13):
                    input_list_2.append(input_10 + str(month) + '.csv')
        files = input_list+input_list_1+input_list_2
        for url in files:
            u = urllib2.urlopen(url, timeout=1000)

            scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
            filename = os.path.basename(path)
            output = os.path.join(data)
            if not os.path.exists(output):
                os.makedirs(output)
            filename = os.path.join(output, filename)
            with open(filename, 'wb') as f:
                meta = u.info()
                meta_func = meta.getheaders if hasattr(
                    meta, 'getheaders') else meta.get_all
                meta_length = meta_func("Content-Length")
                file_size = None
                if meta_length:
                    file_size = int(meta_length[0])
                    print("Downloading: {0} Bytes: {1}".format(url, file_size))
                file_size_dl = 0
                block_sz = 8192
                while True:
                    buffer = u.read(block_sz)
                    if not buffer:
                        break

                    file_size_dl += len(buffer)
                    f.write(buffer)

                    status = "{0:16}".format(file_size_dl)
                    if file_size:
                        status += "   [{0:6.2f}%]".format(
                            file_size_dl * 100 / file_size)
                    status += chr(13)
                    print(status, end="")
        print(" -- Completed downloading files for -- {}".format(data))
    elif data == 'Green':
        input_list = list()
        input_list_1 = list()
        input_list_2 = list()
        for year in args:
            if year == 2021:  # if the last year is given, the datasts only for the last 6 month will be extracted
                input = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-0'
                for month in range(1, 7):
                    input_list.append(input + str(month) + '.csv')
            elif year == 2020:
                input = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2020-0'
                for month in range(1, 10):
                    input_list_1.append(input + str(month) + '.csv')
                input_10 = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2020-'
                for month in range(10, 13):
                    input_list_1.append(input_10 + str(month) + '.csv')
            elif year == 2019:
                input = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2019-0'
                for month in range(1, 10):
                    input_list_2.append(input + str(month) + '.csv')
                input_10 = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2019-'
                for month in range(10, 13):
                    input_list_2.append(input_10 + str(month) + '.csv')

        files = input_list+input_list_1+input_list_2
        for url in files:
            u = urllib2.urlopen(url, timeout=1000)

            scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
            filename = os.path.basename(path)
            output = os.path.join(data)
            if not os.path.exists(output):
                os.makedirs(output)
            filename = os.path.join(output, filename)
            with open(filename, 'wb') as f:
                meta = u.info()
                meta_func = meta.getheaders if hasattr(
                    meta, 'getheaders') else meta.get_all
                meta_length = meta_func("Content-Length")
                file_size = None
                if meta_length:
                    file_size = int(meta_length[0])
                    print("Downloading: {0} Bytes: {1}".format(url, file_size))
                file_size_dl = 0
                block_sz = 8192
                while True:
                    buffer = u.read(block_sz)
                    if not buffer:
                        break

                    file_size_dl += len(buffer)
                    f.write(buffer)

                    status = "{0:16}".format(file_size_dl)
                    if file_size:
                        status += "   [{0:6.2f}%]".format(
                            file_size_dl * 100 / file_size)
                    status += chr(13)
                    print(status, end="")
        print("-- Completed downloading files for --{}".format(data))
    path_downloaded = os.path.join(data)
    path_downloaded = os.path.abspath(path_downloaded)
    files_downloaded = glob.glob(str(path_downloaded)+'\*')
    return files_downloaded

#====================================================================================================#
# Main Body
#====================================================================================================#


if __name__ == "__main__":

    data_set_for = 'Yellow'

    # settings for GCP Dataflow/Spark
    PROJECT_ID = "MobiLab_tech_task"
    SCHEMA_MAP = {

    }
    aa = 'VendorID:INTEGER, tpep_pickup_datetime:TIMESTAMP, tpep_dropoff_datetime:TIMESTAMP\
                Passenger_count:INTEGER, Trip_distance:FLOAT, PULocationID:INTEGER, DOLocationID:INTEGER \
                RateCodeID:INTEGER, Payment_type:INTEGER, Fare_amount:FLOAT, Extra:FLOAT, MTA_tax:FLOAT\
                Improvement_surcharge:FLOAT, Tip_amount:FLOAT, Tolls_amount:FLOAT, Total_amount:FLOAT'
    # output = '/home/mobilab-task'

    # Pipeline options

    class MyOptions(PipelineOptions):
        """Creating settings for Pipeline than need to be executed externally"""
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument('--input',
                                help='Input for the pipeline',
                                default='')
            parser.add_argument('--output',
                                help='Output for the pipeline',
                                default='')

    options = PipelineOptions(
        # ["--runner=PortableRunner",
        #                        "--job_endpoint=localhost:8099",
        #                        "--environment_type=LOOPBACK"]
    )
    p = beam.Pipeline(options=options)
    # downloaded = reading_files_from_url(data_set_for, 2021, 2020, 2019)
    path_downloaded = os.path.join(data_set_for)
    path_downloaded = os.path.abspath(path_downloaded)
    files_downloaded = glob.glob(str(path_downloaded)+'\*')

    input = files_downloaded
    print(input)
    output = os.path.join('output/'+data_set_for)
    # if not os.path.exists(output):
    #     os.makedirs(output)
    print("-- Constructing the Pipeline --")
    file_content = []  # creating empty set that will be used as a number of PCollection
    for i, file in enumerate(input):
        """Creating loop to iterate over files in the input folder"""
        if i == 0:  # for the first file the header is selected and for the rest it is omitted
            reading = (p
                       | "Reading File{}".format(file) >> beam.io.ReadFromText(file)
                       )
        else:
            reading = (p
                       | "Reading File{}".format(file) >> beam.io.ReadFromText(file, skip_header_lines=1)
                       )
        file_content.append(reading)

    writting = (file_content  # after reading all files, all are merged into a final file
                | beam.Flatten()
                | beam.io.WriteToText(output, file_name_suffix='.csv'))

    p.run()
    print("-- Pipeline was successfully constructed!! --")
