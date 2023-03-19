from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, IntegerType
import timeit

spark = SparkSession.builder.appName("exp0_df_v1").getOrCreate()

def count_in_a_partition(iterator):
  yield sum(1 for _ in iterator)

def create_dataframe():
    global df
    lines = spark.read.text("hdfs://rce:9001/user/hdfs/final.sam")
    filtered_lines = lines.filter(udf(lambda x: x.startswith("@") == False, BooleanType())(lines.value))
    extract_udf = udf(lambda x : int(x.strip().split('\t')[3]), IntegerType())
    df = filtered_lines.withColumn("pos", extract_udf(col("value"))).persist(StorageLevel.MEMORY_ONLY)
    #print(df.rdd.mapPartitions(count_in_a_partition).collect())

def count_rdd(x):
    print(x.rdd.count())

def time_function(f, arg, n):
    return timeit.Timer(lambda: f(arg)).timeit(n)/n

# print("Creating dataframe")
# total = timeit.timeit(create_dataframe, number=1)
# overhead = time_function(count_rdd, df, 5)
# print("Create: ", total, overhead, total - overhead)
create_dataframe()

def sort_df():
    global s_df
    s_df = df.sort(col("pos"))
    print(s_df.rdd.count())
    #print(s_df.rdd.mapPartitions(count_in_a_partition).collect())

# total = timeit.timeit(sort_df, number=1)
# overhead = time_function(count_rdd, s_df, 5)
# print("Sort: ", total, overhead, total - overhead)
sort_df()
