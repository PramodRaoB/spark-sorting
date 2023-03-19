from pyspark import SparkContext
import timeit

NUM_PARTITIONS = 300

sc = SparkContext(appName='exp0_rdd_v1') # look into profiler parameter to SparkContext

def create_dataframe():
    global rdd
    lines = sc.textFile("hdfs://rce:9001/user/hdfs/final.sam")
    filtered_lines = lines.filter(lambda x: x.startswith("@") == False)
    rdd = filtered_lines.map(lambda x: (int(x.split('\t')[3]), x)).cache()

create_dataframe()

def count_in_a_partition(iterator):
  yield sum(1 for _ in iterator)

def count_rdd(x):
    print(x.count())

def time_function(f, arg, n):
    return timeit.Timer(lambda: f(arg)).timeit(n)/n

def sort_rdd():
    global partitioned_rdd
    partitioned_rdd = rdd.sortByKey()
    print(partitioned_rdd.count())
    #print(partitioned_rdd.mapPartitions(count_in_a_partition).collect())

# total = timeit.timeit(sort_rdd, number=1)
# overhead = time_function(count_rdd, partitioned_rdd, 5)
# print("Sort: ", total, overhead, total - overhead)
sort_rdd()
