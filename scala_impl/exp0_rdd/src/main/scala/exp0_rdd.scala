import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object exp1_rdd {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("exp0_rdd_v2")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer.objectStreamReset", "100")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(Array(classOf[scala.Tuple2[Int, String]], Class.forName("scala.reflect.ClassTag$GenericClassTag")))
    val sc = SparkContext.getOrCreate(sparkConf)

    val input_file = "hdfs://rce:9001/user/hdfs/data.sam"

    val lines = sc.textFile(input_file)
    val filtered_lines = lines.filter(line => line.startsWith("@") == false)
    // val rdd : RDD[kv_pair] = filtered_lines.map(x => kv_pair(x.split('\t')(3).toInt, x)).persist(StorageLevel.MEMORY_ONLY_SER)
    val rdd : RDD[scala.Tuple2[Int, String]] = filtered_lines.map(x => (x.split('\t')(3).toInt, x)).persist(StorageLevel.MEMORY_ONLY_SER)

    val final_rdd = rdd.sortByKey()
    print(final_rdd.count())
    sc.stop()
  }
}
