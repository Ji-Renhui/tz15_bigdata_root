package tz15_bigdata_root.spark.sparkstreaming.kafka.warn

import java.util.Timer

import org.apache.spark.storage.StorageLevel
import tz15_bigdata_root.spark.common.SscFactory
import tz15_bigdata_root.spark.kafka.config.KafkaConfig
import tz15_bigdata_root.spark.sparkstreaming.kafka.utils.KafkaHelper
import tz15_bigdata_root.spark.warn.timer.SyncRule2RedisTimer

/**
 * @ClassName WarnStreaming
 * @Description
 * @Date 2021/2/22 9:49
 * @Created by robot
 **/
object WarnStreaming {
  def main(args: Array[String]): Unit = {
    val timer = new Timer()
    timer.schedule(new SyncRule2RedisTimer(),0,1*3*1000)

    val topic = "test"
    val groupId = "Kafka2esStreaming"
    val ssc = SscFactory.newLocalSSC("Kafka2esStreaming", 5L, 2)
    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)
    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams, groupId, topic)
            .persist(StorageLevel.MEMORY_AND_DISK)
    //TODO 布控预警
    BlackWarnTask.beginWarn(DS)
    //TODO 流量预警
    FlowWarnTask.beginWarn(DS)

    ssc.start()
    ssc.awaitTermination()

  }
}