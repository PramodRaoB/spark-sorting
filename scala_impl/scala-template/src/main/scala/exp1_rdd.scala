import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object exp1_rdd {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("exp0_df_v2").getOrCreate()
    import spark.implicits._
    val input_file = "hdfs://rce:9001/user/hdfs/data1.sam"
    val lines = spark.read.textFile(input_file)
    val filtered_lines = lines.filter(line => line.startsWith("@") == false)
    val rdd = filtered_lines.map(x => (x.split('\t')(3).toInt, x)).cache()

    var max_val = 0
    /*
     * To figure out the max value of rdd:
     * max_val = rdd.agg(max("_1")).as[Integer].first
     * */

    // Below for data1
    val final_rdd = rdd.sort("_1")
    print(final_rdd.rdd.count())
    spark.stop()
  }
}
