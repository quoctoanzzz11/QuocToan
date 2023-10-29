
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
import pyspark.sql.functions as sf
import os 
import datetime 
from pyspark.sql.functions import col, greatest , when, concat_ws, count, sum, lit, round


spark = SparkSession.builder.config("spark.driver.memory","4g").getOrCreate()


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

def generate_date_range(from_date,to_date):                         # Tạo list theo khoảng thời gian
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)
	date_list = date_range(from_date,to_date)
	return date_list



def calculate_days_between_dates(from_date,to_date):
    date_format = "%Y%m%d"
    fromdate = datetime.datetime.strptime(from_date, date_format)
    todate = datetime.datetime.strptime(to_date, date_format)

    delta = todate - fromdate 
    
    return delta.days + 1


def etl(df):
    df = df.select('_source.Contract','_source.AppName','_source.TotalDuration')
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
    df = df.withColumnRenamed('Movie','MovieDuration')
    df = df.withColumnRenamed('Relax','RelaxDuration')
    df = df.withColumnRenamed('Sport','SportDuration')
    df = df.withColumnRenamed('TV','TVDuration')
    return df


def most_watch(df):
    max_value = greatest(col("ChildDuration"), col("MovieDuration"), col("RelaxDuration"), col("SportDuration"), col("TVDuration"))
    df = df.withColumn("Most_Watch", when(max_value == col("ChildDuration"), "Child")
                                   .when(max_value == col("MovieDuration"), "Movie")
                                   .when(max_value == col("RelaxDuration"), "Relax")
                                   .when(max_value == col("SportDuration"), "Sport")
                                   .when(max_value == col("TVDuration"), "TV"))
    return df



def customer_state(df):
    customer_state = concat_ws("-", 
    when(col("ChildDuration") > 0, "Child"),
    when(col("MovieDuration") > 0, "Movie"),
    when(col("RelaxDuration") > 0, "Relax"),
    when(col("SportDuration") > 0, "Sport"),
    when(col("TVDuration") > 0, "TV")
    )

    df = df.withColumn("Customer_Taste", customer_state)

    df = df.replace(['Child-Movie-Relax-Sport-TV'],['all'])
    
    return df 


def activeness(from_date,to_date):
    path = "E:\\log_content\\"
    dinh_dang = '.json'
    list_file_1 = generate_date_range(from_date,to_date)

    file_name = list_file_1[0]
    result1 = spark.read.json(path+file_name+dinh_dang)

    result1 = etl(result1)
    result1 = result1.withColumn('date', lit(from_date))  #Thêm date 

    
    for i in list_file_1[1:]:
        file_name2 = i
        result2 = spark.read.json(path+file_name2+dinh_dang)
        result2 = etl(result2)

        result2 = result2.withColumn('date', lit(i))  #Thêm date ở mỗi file
 
        result1 = result1.union(result2)
        result1 = result1.cache()

    #Output sau vòng for: Contract, ChildDuration, MovieDuration, RelaxDuration, SportDuration, TVDuration, date

    #Tính total thời gian xem một người trong 1 ngày
    result1 = result1.withColumn("total", col("ChildDuration") + col("MovieDuration") + col("RelaxDuration") + col("SportDuration") +  col("TVDuration")) 
    # nếu ngày nào có thời gian sử dụng (total) > 0 --> activeness đếm
    result1 = result1.groupBy("Contract").agg(count(when(col("total") > 0, 1)).alias("Activeness"))

    # Tỷ Lệ Phần Trăm (Số Ngày Sử Dụng)/(Số Ngày Trong Một Khoảng Thời Gian)
    z = calculate_days_between_dates(from_date,to_date)

    result1 = result1.withColumn("Activeness", round(col("Activeness") / z, 2))

    return result1 # Trả về 2 cột Contract và Activeness


def mw_cs(from_date,to_date):
    path = "E:\\log_content\\"
    dinh_dang = '.json'
    
    file_name1 = spark.read.json(path+from_date+dinh_dang)
    
    list_file_1 = generate_date_range(from_date,to_date)   

    for i in list_file_1[1:]:
        file_name2 = spark.read.json(path+i+dinh_dang)
        
        file_name1 = file_name1.union(file_name2)
        file_name1 = file_name1.cache()
    
    final = etl(file_name1)
    final = most_watch(final)
    final = customer_state(final)

    return final #Trả về Các cột gồm Contract, ChildDuration, MovieDuration, RelaxDuration, SportDuration, TVDuration, Most_Watch, Customer_Taste



def main_task(from_date,to_date):
     temp1 = activeness(from_date,to_date)
     temp2 = mw_cs(from_date,to_date)

     #Có cùng contract --> Outer Join
     temp3 = temp2.join(temp1, on="Contract", how="outer")

     final = temp3.select('Contract','Most_Watch','Customer_Taste','Activeness')
     
     return final
    
temp = main_task('20220401','20220404')
temp.show()

