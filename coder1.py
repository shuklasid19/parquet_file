from pyspark.sql import *



def session():
    spark = SparkSession.builder.master('local[3]').appName('coder1').getOrCreate()
    return spark
        

def data_creator( spark, data , val, column_names=False):
    data = spark.read.options(delimiter = val, header = column_names).csv(data)
    return data

#start the session
spark = session()

#data file location
data_loc = r'C:\Users\sid\Downloads\code pil\user.csv'

#seperator
val=";"

#header parameter
column_names=True

#function
create_data_creator = data_creator(spark , data_loc, val , column_names)

create_data_creator.show()
#create_data_creator.write.parquet("data_file")

create_data_creator.write.parquet('file_converted_to_parquet.parquet')


#def parquet(data, format):
#    if format='parquet':
#        parquet = data.to_csv('parquet.csv')
#    else:
#        return parquet


