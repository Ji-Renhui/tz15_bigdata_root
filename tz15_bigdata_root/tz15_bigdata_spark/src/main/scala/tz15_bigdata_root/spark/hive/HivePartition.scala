package tz15_bigdata_root.spark.hive

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import tz15_bigdata_root.common.time.TimeTranstationUtils
import tz15_bigdata_root.spark.common.SessionFactory
import tz15_bigdata_root.spark.common.convert.DataConvert
import tz15_bigdata_root.spark.kafka.config.KafkaConfig
import tz15_bigdata_root.spark.sparkstreaming.kafka.utils.KafkaHelper

import scala.collection.JavaConversions._
/**
 * @ClassName HivePartition
 * @Description
 * @Date 2021/2/24 22:49
 * @Created by robot
 **/
object HivePartition extends Serializable with Logging{


  def main(args: Array[String]): Unit = {
    //TODO 1.连接HIVE，初始化HIVE表。 动态生成HIVE  SQL
    val spark = SessionFactory.newLocalHiveSession("HivePartition", 2)
    spark.sql("use default")
    HiveConfigPar.hiveParSql.foreach(map => {
      spark.sql(map._2)
    })

    //TODO 2. 将kafka的数据写入到HDFS   写入parquent格式数据 DF
    val topic = "test"
    val groupId = "Kafka2esStreaming"
    //val ssc = SscFactory.newLocalSSC1(sparkConf, 5L)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)
    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams, groupId, topic)
      .map(map => {
        //添加分区字段
        //添加日期字段，为了按日期分组
        val collect_time = map.get("collect_time") //采集时间  时间戳格式
        // YYYY-MM-DD  引入时间转换工具类
        val date = TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(collect_time + "000"))
        val year = date.substring(0, 4)
        val month = date.substring(4, 6)
        val day = date.substring(6, 8)
        map.put("dayPartion", date)
        map.put("year", year)
        map.put("month", month)
        map.put("day", day)
        map
      })


    HiveConfig.tables.foreach(table => {
      //1.按类型分组
      val tableDS = DS.filter(map => {
        table.equals(map.get("table"))
      })
      tableDS.foreachRDD(rdd => {


        val tableSchema = HiveConfig.mapSchema.get(table.toString)
        val schemaFields = tableSchema.fieldNames
        //TODO 按日期进行分组
        val arrayDays = rdd.map(x => (x.get("dayPartion"))).distinct().collect() //把所有数据汇集到driver端
        arrayDays.foreach(date => {

          val year = date.substring(0, 4)
          val month = date.substring(4, 6)
          val day = date.substring(6, 8)

          //TODO 3.进一步按日期过滤
          val rowRDD = rdd.filter(map => {
            date.equals(map.get("dayPartion"))
          }).map(map => {
            //数据类型转换
            //把MAP转为row
            val list = new util.ArrayList[Object]()
            for (schemaField <- schemaFields) {
              list.add(map.get(schemaField))
            }
            Row.fromSeq(list)
          })


          //RDD => DF  按表去写
          //定义StructType  放到初始化中去做

          val tableDF = spark.createDataFrame(rowRDD, tableSchema)
          tableDF.show(2)
          //构建完成DF，开始往HDFS写入
          //定义HDFS目录
          val tableHdfspATH = s"hdfs://mycluster${HiveConfig.rootPath}/${table}/${year}/${month}/${day}"
          //   /user/hive/warehouse/wechat/year/month/day
          tableDF.write.mode(SaveMode.Append).parquet(tableHdfspATH)
          //TODO 3.建立HDFS 与 hive 之间的映射关系
          //设置映射关系
          val sql = s"ALTER TABLE ${table}  ADD IF NOT EXISTS PARTITION (year='${year}',month='${month}', day='${day}')  LOCATION '${tableHdfspATH}'"
          spark.sql(sql)
          println(sql)
        })
      })

      ssc.start()
      ssc.awaitTermination()
    })
  }
}
