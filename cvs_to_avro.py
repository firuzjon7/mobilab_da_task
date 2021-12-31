from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import avro.schema
import os
import apache_beam as beam
from avro import schema
import avro
from apache_beam.io import ReadFromAvro
from apache_beam.io import WriteToAvro

schema = avro.schema.parse(open("avro_yellow.avsc", "rb").read())

p = beam.Pipeline()
converting = (p
              | beam.io.ReadFromText(os.path.join('/output/Yellow-00000-of-00001.csv'))
              | beam.Map(lambda element:   element)
              | beam.io.WriteToAvro(os.path.join('/output/yellow.avro)', schema=schema)))
p.run()
