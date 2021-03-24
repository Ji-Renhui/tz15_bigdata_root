package tz15_bigdata_root.spark.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging

/**
 * @ClassName SparkContextFactory
 * @Description
 * @Date 2021/2/20 15:08
 * @Created by robot
 **/
object SparkContextFactory extends Serializable with Logging{

  /**
   * 本地模式
   * @param appName
   * @param threads
   * @return
   */
  def newLocalSparkContext(appName:String="default",threads:Int=2): SparkContext ={
    val sparkConf = SparkConfFactory.newLocalSparkConf(appName,threads)
    return new SparkContext(sparkConf)
  }


  /**
   * 集群模式
   */
  def newYarnSparkContext(): SparkContext ={
    val sparkConf = SparkConfFactory.newYarnSparkConf()
    return new SparkContext(sparkConf)
  }
}
