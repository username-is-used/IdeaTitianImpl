package org.apache.spark.lineage.Application

//import org.apache.spark.lineage.util.LineageContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
  * Created by cz on 16-4-12.
  */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("WordCount-" ).setMaster("local")
    val sc = new SparkContext(conf)
//    val lc = new LineageContext(sc)  // To enable tracing, use lineage context to run your application
    val file = sc.textFile("/home/cz/Hadoop/spark-learn/spark-all/learning-spark-example/README.md")
//    file.collect().foreach(println)
    val fm = file.flatMap(line =>line.split("t"))
    val pair = fm.map{word => (word, 1)}
    val count = pair.reduceByKey(_ + _)
    val d = count.collect().foreach(println)
  }
}
