#Method 2 (tổng hợp toàn bộ --> xử lý)

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
import pyspark.sql.functions as sf
import os 
import datetime 

spark = SparkSession.builder.config("spark.driver.memory","2g").getOrCreate()

def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value

def date_range(start_date,end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list

def generate_date_range(from_date,to_date):
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)
	date_list = date_range(from_date,to_date)
	return date_list


def etl_file(df):
    df = df.withColumn("Type",
       when((col("AppName") == 'CHANNEL') |  (col("AppName") =='KPLUS'), "TV")
      .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS') , "Movie")
      .when((col("AppName") == 'CHILD'), "Child")
      .when((col("AppName") == 'RELAX'), "Relax")
      .when((col("AppName") == 'SPORT'), "Sport")
      .otherwise("Error"))
    df = df.drop(df.AppName)
    df = df.groupBy('Contract').pivot('Type').sum('TotalDuration')
    df = df.fillna(0)
    df = df.withColumnRenamed('Child','ChildDuration')
    ds = df.withColumnRenamed('Movie','MovieDuration')
    df = df.withColumnRenamed('Relax','RelaxDuration')
    df = df.withColumnRenamed('Sport','SportDuration')
    df = df.withColumnRenamed('TV','TVDuration')
    return df


def main_task(from_date,to_date):
    path = "E:\\log_content\\"
    dinh_dang = '.json'
    file_name1 = spark.read.json(path+from_date+dinh_dang)
    file_name1 = file_name1.select('_source.Contract','_source.AppName','_source.TotalDuration')
    
    list_file = os.listdir(path)
    list_file_1 = generate_date_range(from_date,to_date)   

    for i in list_file_1[1:]:
        file_name2 = spark.read.json(path+i+dinh_dang)
        file_name2 = file_name2.select('_source.Contract','_source.AppName','_source.TotalDuration')
        
        file_name1 = file_name1.union(file_name2)
        file_name1 = file_name1.cache()
    
    final = etl_file(file_name1)
    
    final.write.csv('E:\\Output_logcontent_method\\clean_data',header=True)

main_task('20220402','20220405')

final = spark.read.csv("E:\\Output_logcontent_method\\clean_data",header=True)
final.show()