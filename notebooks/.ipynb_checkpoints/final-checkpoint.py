import pyspark
from pyspark.sql import SparkSession,Window
from pyspark.conf import SparkConf
from pyspark.sql.functions import col,lit,explode,count,when,min,max,avg,trim,sum,struct,round,broadcast,udf
from pyspark.sql.types import StringType

def remove_spaces(s):
    return s.replace(' ','_')

def extract_year(s):
    return (s.split('-')[1])

def extract_country(s):
    return (s.split('-')[0])

def order_id_null(spark,df):
    null_user_id_count = df.filter(col("order_id").isNull()).count()
    assert null_user_id_count == 0, f"order_id null test Failed"
    print("order_id null test - Success")


def main():
    spark = SparkSession.builder.appName('Yagro Interview')\
                    .config("spark.jars.packages", 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.0')\
                    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017") \
                    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
                    .getOrCreate()

    df=spark.read.csv('/home/jovyan/data/YAGRO-fuel-sales.csv')

    rows = df.collect()

    main_columns =[]
    for i in range(1, len(rows[0])):
    	if(rows[0][i] is not None and 'Total' not in  rows[0][i]):
       		main_columns.append(rows[0][i])
            
    # for each main column get the subcolumns and their location
    col_location =[]
    current_main_col = main_columns.pop(0)
    for i in range(1, len(rows[1])):
        if(rows[1][i] is None):
            col_location.append((current_main_col, remove_spaces(rows[0][i]),i))
            try:
                current_main_col = main_columns.pop(0)
            except IndexError:
                pass
        else:
            col_location.append((current_main_col, remove_spaces(rows[1][i]),i))
    print(col_location)
    rdd_with_index = df.rdd.zipWithIndex()
    df_with_index = rdd_with_index.map(lambda x: (*x[0], x[1])).toDF(df.columns + ["index"])

    # get index of last row
    last_row = df_with_index.agg(max("index").alias("max_index")).collect()[0][0]

    df_new = df_with_index.filter((df_with_index.index > 2) & (df_with_index.index != last_row)).drop('index')

    df_new = df_new.withColumnRenamed('_c0','order_id')
    
    for i in col_location:
        if(i[0] not in df_new.columns):
            df_new = df_new.withColumn(i[0], struct(col(f"_c{i[2]}").alias(i[1]))).drop(f"_c{i[2]}")
        else:
            df_new = df_new.withColumn(i[0],col(i[0]).withField(i[1], col(f"_c{i[2]}").alias(i[1]))).drop(f"_c{i[2]}")

    extract_year_udf = udf(extract_year, StringType())
    extract_country_udf = udf(extract_country, StringType())
    df_new = df_new.withColumn('year',extract_year_udf(col('order_id')))
    df_new = df_new.withColumn('country',extract_country_udf(col('order_id')))

    # sample test
    order_id_null(spark,df_new)

    df_new.write \
    .format("mongodb") \
    .option("uri", "mongodb://mongodb:27017/ds_area.interview_dataset") \
    .option("database", "ds_area") \
    .option("collection", "full_data") \
    .mode('overwrite') \
    .save()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
