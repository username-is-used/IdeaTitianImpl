package org.apache.spark.lineage.rdd

import java.io.EOFException
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{InputMetrics, DataReadMethod}
import org.apache.spark.util.NextIterator
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.lineage.util.LineageContext
import org.apache.hadoop.mapred._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD}

/**
  * Created by cz on 16-4-12.
  */
class HadoopLineageRDD[K,V](lineageContext : LineageContext,
                            broadcastedConf: Broadcast[SerializableWritable[Configuration]],
                            initLocalJobConfFuncOpt: Option[JobConf => Unit],
                            inputFormatClass: Class[_ <: InputFormat[K, V]],
                            keyClass: Class[K],
                            valueClass: Class[V],
                            minPartitions: Int)
   extends HadoopRDD[K,V](lineageContext.getSparkContext(),
      broadcastedConf,
      initLocalJobConfFuncOpt,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions){
  override def compute(theSplit: Partition, context: TaskContext):  InterruptibleIterator[(K, V)]  = {
    val iter = new NextIterator[(K, V)] {
      val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      val jobConf = getJobConf()

      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = if (split.inputSplit.value.isInstanceOf[FileSplit]) {
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback(
          split.inputSplit.value.asInstanceOf[FileSplit].getPath, jobConf)
      } else {
        None
      }

      var reader: RecordReader[K, V] = null
      val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format( new Date()),
        context.stageId, theSplit.index, context.attemptId.toInt, jobConf)
      reader = inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }
      val key: K = reader.createKey()
      val value: V = reader.createValue()

      var recordsSinceMetricsUpdate = 0

      override def getNext() = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eof: EOFException =>
            finished = true
        }

        // Update bytes read metric every few records
        if (recordsSinceMetricsUpdate == HadoopRDD.RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES
          && bytesReadCallback.isDefined) {
          recordsSinceMetricsUpdate = 0
          val bytesReadFn = bytesReadCallback.get
          inputMetrics.bytesRead = bytesReadFn()
        } else {
          recordsSinceMetricsUpdate += 1
        }
        (key, value)
      }

      override def close() {
        try {
          reader.close()
          if (bytesReadCallback.isDefined) {
            val bytesReadFn = bytesReadCallback.get
            inputMetrics.bytesRead = bytesReadFn()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.bytesRead = split.inputSplit.value.getLength
              context.taskMetrics.inputMetrics = Some(inputMetrics)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        } catch {
          case e: Exception => {

              logWarning("Exception in RecordReader.close()", e)
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

}
