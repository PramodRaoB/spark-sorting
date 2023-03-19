from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import BooleanType, IntegerType, StringType
import timeit


print("Begun")
spark = SparkSession.builder.appName("sorting sam").getOrCreate()

def count_rdd(x):
    print(x.rdd.count())

def create_dataframe():
    global df
    lines = spark.read.text("hdfs://rce:9001/user/hdfs/data.sam")
    filtered_lines = lines.filter(udf(lambda x: x.startswith("@") == False, BooleanType())(lines.value))
    extract_udf = udf(lambda x : int(x.strip().split('\t')[3]), IntegerType())
    df = filtered_lines.withColumn("pos", extract_udf(col("value")))
    df = df.select("pos", lit("ABCD"))
    count_rdd(df)

def time_function(f, arg, n):
    return timeit.Timer(lambda: f(arg)).timeit(n)/n

print("Creating dataframe")
total = timeit.timeit(create_dataframe, number=1)
#overhead = time_function(count_rdd, df, 5)
#print("Create: ", total, overhead, total - overhead)

def sort_df():
    global s_df
    s_df = df.sort(col("pos"))
    count_rdd(s_df)
    #s_df.select("value").write.format("text").option("header", "false").mode("overwrite").save("hdfs://rce:9001/user/hdfs/sorted.txt")

print("Sorting dataframe")
total = timeit.timeit(sort_df, number=1)
#overhead = time_function(count_rdd, s_df, 5)
#print("Sort: ", total, overhead, total - overhead)
input()
