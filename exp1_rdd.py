from pyspark import SparkConf, SparkContext

NUM_PARTITIONS = 10

# conf = SparkConf().setAppName('exp1_rdd_v1').set('spark.shuffle.file.buffer', '64k')
# sc = SparkContext(conf=conf)
sc = SparkContext(appName='exp1_rdd_v1') # look into profiler parameter to SparkContext

def create_dataframe():
    global rdd
    lines = sc.textFile("hdfs://rce:9001/user/hdfs/data1.sam")
    filtered_lines = lines.filter(lambda x: x.startswith("@") == False)
    rdd = filtered_lines.map(lambda x: (int(x.split('\t')[3]), x)).cache()

#print("Creating dataframe")
#print("Create:", timeit.timeit(create_dataframe, number=1))
create_dataframe()

def count_in_a_partition(iterator):
  yield sum(1 for _ in iterator)


def repartition_rdd():
    global partitioned_rdd
    """
        Do the following to obtain max value
        max_val = rdd.max()[0]
    """
    # max_val = rdd.max()[0]
    # print(max_val)
    #below for data0, data1
    max_val = 249240341
    #below for data4 and data
    # max_val = 249240474
    #below for final.sam
    # max_val = 248946383
    partitioned_rdd = rdd.repartitionAndSortWithinPartitions(NUM_PARTITIONS, lambda x: x // (max_val // NUM_PARTITIONS), True)
    # div = 10
    # partitioned_rdd = rdd.repartitionAndSortWithinPartitions(NUM_PARTITIONS + div, lambda x: ((x // (max_val // NUM_PARTITIONS)) // div) if (x // (max_val // NUM_PARTITIONS) <= 0.1 * NUM_PARTITIONS) else (x // (max_val // NUM_PARTITIONS) + div), True)
    # print(partitioned_rdd.count())
    #print(partitioned_rdd.mapPartitions(count_in_a_partition).collect())
    partitioned_rdd.saveAsTextFile("hdfs://rce:9001/user/hdfs/partrdd2")

#print("Repartitioning")
#print("Repartition:", timeit.timeit(repartition_rdd, number=1))
repartition_rdd()
