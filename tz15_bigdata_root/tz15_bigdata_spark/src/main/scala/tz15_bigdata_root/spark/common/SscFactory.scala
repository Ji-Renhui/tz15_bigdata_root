package tz15_bigdata_root.spark.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @ClassName SscFactory
 * @Description
 * @Date 2021/2/20 15:28
 * @Created by robot
 **/
object SscFactory extends Serializable with Logging {

  /**
   * newLocalSSC
   * @param appName
   * @param batchInterval
   * @param threads
   * @return
   */

  def newLocalSSC(appName: String = "default", batchInterval: Long = 10L, threads: Int = 2): StreamingContext = {
    val sparkConf = SparkConfFactory.newLocalSparkConf(appName, threads)
    sparkConf.set("spark.streaming.receiver.maxRate", "1000")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    new StreamingContext(sparkConf, Seconds(batchInterval))
  }
  def newClusterSSC( batchInterval: Long = 10L): StreamingContext = {
    val sparkConf = SparkConfFactory.newYarnSparkConf()
    sparkConf.set("spark.streaming.receiver.maxRate", "1000")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    new StreamingContext(sparkConf, Seconds(batchInterval))

  }

}
