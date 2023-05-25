import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.RangePartitioner

object exp1_rdd {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("exp4_rdd_v2")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer.objectStreamReset", "100")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(Array(classOf[scala.Tuple2[Int, String]], Class.forName("scala.reflect.ClassTag$GenericClassTag")))
    val sc = SparkContext.getOrCreate(sparkConf)

    val input_file = "hdfs://rce:9001/user/hdfs/final.sam"

    val lines = sc.textFile(input_file)
    val read_lines = lines.filter(line => line.startsWith("@") == false)
    val header_lines = lines.filter(line => line.startsWith("@SQ") == true)

    // Create header dict
    var dict:Map[String, Long] = Map()
    var pref = 0.toLong

    for (header <- header_lines.collect()) {
      val name = (header.split('\t')(1)).substring(3)
      val len = (header.split('\t')(2)).substring(3).toLong
      dict += (name -> pref)
      pref += len
    }
    dict += ("*" -> pref)

    // val rdd : RDD[scala.Tuple2[Int, String]] = filtered_lines.map(x => (x.split('\t')(3).toInt, x)).persist(StorageLevel.MEMORY_ONLY_SER)
    // val rdd: RDD[scala.Tuple2[Int, Array[Byte]]] = filtered_lines.map(x => (x.split('\t')(3).toInt, x.getBytes)).persist(StorageLevel.MEMORY_ONLY_SER)
    val rdd: RDD[scala.Tuple2[Long, String]] = read_lines.map(x => (x.split('\t')(3).toLong + dict(x.split('\t')(2)), x)).persist(StorageLevel.MEMORY_ONLY_SER)

    val NUM_PARTITIONS = 2700
    var size_of_partition = (pref / NUM_PARTITIONS) + 1
    if (size_of_partition % 1000 != 0) size_of_partition = (size_of_partition / 1000 + 1) * 1000

    class EqualPartitioner(numberOfPartitions: Int) extends Partitioner {
      override def numPartitions: Int = numberOfPartitions
      override def getPartition(key: Any): Int = {
        (key.asInstanceOf[Long] / size_of_partition).toInt
      }
    }

    val partitioned_rdd = rdd.repartitionAndSortWithinPartitions(new EqualPartitioner(NUM_PARTITIONS))
    // val partitioner = new RangePartitioner(NUM_PARTITIONS, rdd, true)
    // val partitioned_rdd = rdd.repartitionAndSortWithinPartitions(partitioner)
    // print(partitioned_rdd.count())
    partitioned_rdd.values.saveAsTextFile("hdfs://rce:9001/user/hdfs/final.out")

    sc.stop()
  }
}
