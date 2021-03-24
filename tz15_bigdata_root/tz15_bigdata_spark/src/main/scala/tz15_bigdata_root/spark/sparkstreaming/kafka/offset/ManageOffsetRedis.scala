package tz15_bigdata_root.spark.sparkstreaming.kafka.offset

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import tz15_bigdata_root.redis.client.JedisUtil

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
/**
 * @ClassName ManageOffsetRedis
 * @Description
 * @Date 2021/2/5 14:00
 * @Created by robot
 **/
object ManageOffsetRedis {
  def saveOffSetToRedis(db: Int,groupId:String,OffsetRanges:Array[OffsetRange]): Unit ={
      val jedis=JedisUtil.getJedis(db)
      OffsetRanges.foreach(OffsetRange=>{
        val redisKey=s"${groupId}_${OffsetRange.topic}"
        jedis.hset(redisKey,OffsetRange.partition.toString,OffsetRange.untilOffset.toString)
      })
  }
  def getOffsetFromRedis(db: Int,groupId: String,topic: String,kafkaParams:java.util.Map[String,Object]): java.util.Map[String,String] ={
    val jedis = JedisUtil.getJedis(db)
    val redisKey = s"${groupId}_${topic}"
    var offsetMap = jedis.hgetAll(redisKey)
    //TODO 如果第一次跑，redis为空怎么处理
    if(offsetMap.size() == 0){
      //TODO 将kafka中最早的偏移初始化到redis中
      offsetMap = initOffset2Redis(topic,kafkaParams.asInstanceOf[java.util.Map[String,Object]])
    }
    jedis.close()
    offsetMap
  }
  def initOffset2Redis(topic: String,kafkaParams:java.util.Map[String,Object]): java.util.Map[String,String] ={
    //kafka的消费者代码
    //TODO 获取当前消费者组在kafka中的偏移，然后存储到redis中
    //获取偏移
    val consumer = new KafkaConsumer(kafkaParams)
    //訂閲主題
    consumer.subscribe(java.util.Arrays.asList(topic))
    consumer.poll(100)
    //获取分区
    val partitions = consumer.assignment()
    //assignment:任务/分配/分派===>分区
    println("partitions"+partitions)
    //获取偏移信息
    consumer.seekToBeginning(partitions)
    consumer.pause(partitions)
    //保存到redis中的格式  key  groupId + topic   hash key值partion  value 偏移量
    val offsets = partitions.map(tp=>{
      tp.partition().toString-> consumer.position(tp).toString
    }).toMap
    consumer.unsubscribe()
    consumer.close()
    offsets
  }



  def updateOffset[T:ClassTag](db: Int, groupId: String, rdd:RDD[T]): Unit ={
    val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    saveOffSetToRedis(db,groupId,offsetRanges)

  }

}
