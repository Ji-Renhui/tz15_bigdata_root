package tz15_bigdata_root.spark.sparkstreaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import tz15_bigdata_root.spark.common.SscFactory


/**
 * @ClassName StreamingKafkaTest
 * @Description
 * @Date 2021/2/4 22:31
 * @Created by robot
 **/
object StreamingKafkaTest {
  def main(args: Array[String]): Unit = {
    val topic = "test"
    val groupId = "StreamingKafkaTest"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata111:9092,bigdata112:9092,bigdata113:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val ssc = SscFactory.newLocalSSC("StreamingKafkaTest", 5L, 2)
//    val sparkConf = new SparkConf().setAppName("StreamingKafkaTest").setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val DS = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set(topic), kafkaParams))
    DS.foreachRDD(rdd => {
      rdd.foreach(line => {
        println(line)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
