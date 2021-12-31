# mobilab_da_task
**Background information**

_The city of New York provides historical data of "The New York City Taxi and Limousine Commission" (TLC). Your colleagues from the data science team want to create various evaluations and predictions based on this data. Depending on their different use cases, they need the output data in a row-oriented and column-oriented format. So they approach to you and ask for your help. Your colleagues only rely on a frequent output of the datasets in these two formats, so you have a free choice of your specific technologies_.


My approach to build a piple will be based on the following technologies:

**BigData tools**: Apache Beam - construct a data pipeline to read cvs files and then merge them into one. Apache Spark - convert data sets into row-based store (Avro) and columnar based store (Parquet). 

**Programming language**: Python is used to dowload files into local machine, construct a pipeline and convert data sets into Avro and Parquet

**Containarization**: Docker containers will be used to run a Spark cluster with two working nodes and a driver

**Possibility**: run the pipeline on the Google Cloud Dataflow

The following diagramm visually describes applied methods and best practices to perform the mentioned task 
![aaaa drawio](https://user-images.githubusercontent.com/62490672/147796300-4829d01b-8417-461e-938b-e2607d2170ea.png)

**Steps to building the pipeline based on the given tasks**

1) Build a data pipeline that imports the TLC datasets for at least the last three years: My apprach to this task was firstly getting all available data sets urls and combining all into a list. To do so, I analyzed the url construction and discovered that all datasets are number according to each month in accending order, e.g. January starts with 01 and October 10 finishing with December 12. Furthermore, I created loop that iterated thorugh 12 months and constructed exact url as the source ones. Simply like below: 
  ```
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
  ```
  According to the data set sources (TLC), data sets for the last year are upload each 6 years. So this case also was taken into account. After creating list of ulrs, each of ulrs is put into a queue for downloading. As the data sets for Yellow Taxi have slightly different schema and different urls, the function for downloading files have to be run twice. 
  ```
  data_set_for = 'Yellow'or 'Green'
  downloaded = reading_files_from_url(data_set_for, 2021, 2020, 2019)
  ```
  2) Enhance your pipeline in that way that it automatically imports future data sets: Yet, downloading files was performed locally, it took a couple of minuites. Indeed, there is a high potential to optimized my code and align it if downloading files will be performed in other sources/buckets/data lakes/etc. Introduced method is capable of importing not only future but also historical data sets by adjusting function arguments and adjusting loops. 
  3)  Convert the input datasets to a column-oriented (e.g. Parquet) and a row-oriented format (e.g. Avro). Before moving to the solution to this task, I would like to introduce the pipleline that was implemented on Apache Beam. After downloading all files and storing them in respective directories, a batch pipeline is created that reads each file from a directory and merge all into one dataset. However, as mentioned before data schema being sligtly different, the pipeline executed twice (for Green and Yellow Taxi datasets). 

```
p = beam.Pipeline(options=options)
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
```
After executing above pipeline two final data sets are generated(as data sets for Yellow Taxi were in huge sizes, only a couple of them were executed along wiht data pipeline): Althought I have tried to run apache beam job with spark runner, there was some technical problem on connecting to a sprak clusted with four docker containers (two worker nodes, a master and a driver node). I have also tried to follow recommendations on how to use an apache spark runner from their [website](https://beam.apache.org/documentation/runners/spark/#additional-notes)   

![image](https://user-images.githubusercontent.com/62490672/147797276-baee1428-566a-4764-8cba-f3b531f68223.png)
Now, my solution to task number 3: I have used Spark environment to convert merged/concatanated files into Parquet and Avro. The source files for converting can be founnd in [Avro](https://github.com/firuzjon7/mobilab_da_task/blob/main/cvs_to_avro.py) and [Parquet](https://github.com/firuzjon7/mobilab_da_task/blob/main/csv_to_parquet.py).

4) Import or define a schema definition.  Schema definition is mostly defined while converting datasets into different file formats(please refer to source codes in task 3). I used mainly data dictionaries to defina schemas for both [Green](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf) and [Yellow](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) data sets. 
5) Structure the data in that way so that the data science team is able to perform their predictions:
 - The input data is spread over several files, including separate files for “Yellow” and “Green” taxis. Does it make sense to merge those input file into one? _**It make sense to merge all data set for a specific taxi. While defining schema, I descovered that there are more columns in Green Taxi data sets. Due to this and and possibly similar values for VendorID, I decided not to merge all data sets into one.** _
 - You will notice that the input data contains “date and time” columns. Your colleagues want to evaluate data also on hour-level and day of week-level. Does that affect your output-structure in some way? _**If an output-structure is referred as a schema, then, yes, there will be more columns and another Transformation and Pcollection if it is implemented by Apache Beam.** _

```
# potential way to split hour and day of the week into separate columns
df['df_hour'] = [d.time() for d in df['timestamp']]
df['df_day_of_the_week'] = [g for n, g in df.set_index('timestamp').groupby(pd.TimeGrouper('W'))]
```
6)To determine the correctness of your output datasets, your colleagues want you to write the following queries: The task is still under progress. Need some time to run final merge on the Spark cluster and then perform queries.


**Bonus:**
1) Your data scientists want to make future predictions based on weather conditions. How would you expand your pipeline to help your colleagues with this task? I would kindly ask my colleagues to provide data sets/or sources for data sets on weather conditions that I could think of integrating into my pipeline. For instance, one idea could be adding weather condition for a each hour of pick up and drop off timestamps. Indeed, all will depend on type of data. 
2) Another colleague approaches to you. He is an Excel guru and makes all kind of stuff using this tool forever. So he needs all the available taxi trip records in the XLSX format. Can you re-use your current pipeline? How does this output compares to your existing formats? Do you have performance concerns? To convert or save data sets into XLSX format won't take much effort to relunch my pipeline. I have to change change an I.O writers to XLSX. The output file will be more sizy compared to others. XLSX format is not friendly with bidg data sets. There is a limit in number rows (if I am not wrong, about 1,5 mn rows). There will be indeed buffering problems while workin on large XLSX files. 


**Note**
I understand that my source codes are not optimized well. Due to short time I tried to write them that they could function in a way that I wanted (hardcoding). Indeed, I will keep improving my code and possibly train better practices. As for now, I belive that during a personal discussion, I can explain more in detail all solutions that were described above. 

 
