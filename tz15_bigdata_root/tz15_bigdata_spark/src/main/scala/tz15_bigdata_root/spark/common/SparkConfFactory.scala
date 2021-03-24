package tz15_bigdata_root.spark.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging

/**
 * @ClassName SparkConfFactory
 * @Description
 * @Date 2021/2/20 15:01
 * @Created by robot
 **/
object SparkConfFactory extends Serializable with Logging {

  /**
   * 本地模式SparkConf
   *
   * @param appName
   * @param threads
   */
  def newLocalSparkConf(appName: String = "default", threads: Int = 2): SparkConf = {
    val sparkConf = new SparkConf().setAppName(appName).setMaster(s"local[${threads}]")
    sparkConf.set("spark.streaming.receiver.maxRate", "1000")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    sparkConf
  }


  /**
   * 集群模式SparkConf
   */
  def newYarnSparkConf(): SparkConf = {
    new SparkConf()
  }
}
