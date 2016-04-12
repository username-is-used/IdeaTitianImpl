package org.apache.spark.lineage.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.lineage.rdd.HadoopLineageRDD
import org.apache.spark.{SparkEnv, SerializableWritable, SparkContext}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import scala.reflect.ClassTag

/**
  * Created by cz on 16-4-12.
  */
class LineageContext(sparkContext : SparkContext) extends scala.AnyRef with org.apache.spark.Logging {

  private[spark] val env = SparkEnv.createDriverEnv(sparkContext.getConf, sparkContext.isLocal, sparkContext.listenerBus)

  def defaultMinPartitions = 2

  def getSparkContext() :SparkContext = sparkContext
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {

    val bc = env.broadcastManager.newBroadcast[T](value, sparkContext.isLocal)
//    val callSite = getCallSite
//    logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
    sparkContext.cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }
  def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {

    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  def hadoopFile[K, V](
                        path: String,
                        inputFormatClass: Class[_ <: InputFormat[K, V]],
                        keyClass: Class[K],
                        valueClass: Class[V],
                        minPartitions: Int = defaultMinPartitions
                      ): RDD[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableWritable(SparkHadoopUtil.get.newConfiguration(sparkContext.getConf)))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopLineageRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }
}
object LineageContext extends scala.AnyRef {
//  type RecordId = scala.Tuple2[scala.Int, scala.Int]
//  val Dummy : scala.Int = { /* compiled code */ }
//  implicit def fromRDDtoLineage(rdd : org.apache.spark.rdd.RDD[_]) : org.apache.spark.lineage.rdd.Lineage[_] = { /* compiled code */ }
//  implicit def fromTapRDDtoLineageRDD(tap : org.apache.spark.lineage.rdd.TapLRDD[_]) : org.apache.spark.lineage.rdd.LineageRDD = { /* compiled code */ }
//  implicit def fromLToLRDD(lineage : org.apache.spark.lineage.rdd.Lineage[_]) : org.apache.spark.lineage.rdd.LineageRDD = { /* compiled code */ }
//  implicit def fromLToLRDD2(lineage : org.apache.spark.lineage.rdd.Lineage[scala.Tuple2[scala.Int, scala.Any]]) : org.apache.spark.lineage.rdd.LineageRDD = { /* compiled code */ }
//  implicit def fromLToLRDD3(lineage : org.apache.spark.lineage.rdd.Lineage[scala.Tuple2[scala.Any, scala.Int]]) : org.apache.spark.lineage.rdd.LineageRDD = { /* compiled code */ }
//  implicit def fromLineageToShowRDD(lineage : org.apache.spark.lineage.rdd.Lineage[scala.Tuple2[LineageContext.RecordId, scala.Predef.String]]) : org.apache.spark.lineage.rdd.ShowRDD[LineageContext.RecordId] = { /* compiled code */ }
//  implicit def lrddToPairLRDDFunctions[K, V](lrdd : org.apache.spark.lineage.rdd.Lineage[scala.Tuple2[K, V]])(implicit kt : scala.reflect.ClassTag[K], vt : scala.reflect.ClassTag[V], ord : scala.Ordering[K] = { /* compiled code */ }) : org.apache.spark.lineage.rdd.PairLRDDFunctions[K, V] = { /* compiled code */ }
//  implicit def lrddToOrderedLRDDFunctions[K, V](lrdd : org.apache.spark.lineage.rdd.Lineage[scala.Tuple2[K, V]])(implicit evidence$1 : scala.Ordering[K], evidence$2 : scala.reflect.ClassTag[K], evidence$3 : scala.reflect.ClassTag[V]) : org.apache.spark.lineage.rdd.OrderedLRDDFunctions[K, V, scala.Tuple2[K, V]] = { /* compiled code */ }
}
