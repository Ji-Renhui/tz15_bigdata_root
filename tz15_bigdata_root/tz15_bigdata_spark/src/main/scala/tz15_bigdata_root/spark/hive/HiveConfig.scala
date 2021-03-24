package tz15_bigdata_root.spark.hive

import org.apache.commons.configuration.{CompositeConfiguration, PropertiesConfiguration}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * @ClassName HiveConfig
 * @Description
 * @Date 2021/2/23 16:03
 * @Created by robot
 **/
object HiveConfig extends Serializable with Logging {
  val hiveFilePath = "es/mapping/fieldmapping.properties"
  val rootPath = "/user/hive/warehouse" //HDFS根目录
  var config: CompositeConfiguration = null
  var tables: java.util.List[_] = null
  var hiveTableSql: java.util.HashMap[String, String] = null //存放表名与SQL之间的映射关系
  var mapSchema: java.util.HashMap[String, StructType] = null //存放每种类型的StructType
  init()

  def main(args: Array[String]): Unit = {

  }

  /**
   * 加载配置文件，初始化tables
   */

  def init(): Unit = {
    println("====================加载配置文件===[hive]==获取所有字段==============")
    config = readCompositeConfiguration(hiveFilePath)
    val keys = config.getKeys
    while (keys.hasNext) {
      println(keys.next())
    }

    println("====================初始化tables===[hive]================")
    tables = config.getList("tables")
    tables.foreach(table => {
      println(table)
    })


    println("====================初始化tables===[hive]================")
    hiveTableSql = getHiveSQL
    hiveTableSql.foreach(table => {
      println(table)
    })


    println("====================初始化mapSchema===[hive]================")
    mapSchema = getSchema
    mapSchema.foreach(x => {
      println(x)
    })

  }

  /**
   * 配置文件读取
   *
   * @param path
   * @return CompositeConfiguration
   *         用这个工具类读取配置文件，配置文件中Key的顺序有序
   *         ConfigUtil 读取的配置文件，Key是无顺序读取的
   */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {
    //配置文件的集合，可以包含多个配置文件
    val compositeConfiguration = new CompositeConfiguration()
    val configuration = new PropertiesConfiguration(path)
    compositeConfiguration.addConfiguration(configuration)
    compositeConfiguration
  }

  def getHiveSQL(): java.util.HashMap[String, String] = {
    val hiveSqlMap = new java.util.HashMap[String, String]
    tables.foreach(table => {
      //开始拼接
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      val fields = config.getKeys(table.toString)
      fields.foreach(tableField => {
        val field = tableField.toString.split("\\.")(1)
        val fieldType = config.getProperty(tableField.toString)
        sql = sql + field
        //模式匹配进行类型转换, 因为ES中的类型和hive中的类型不是一一对应的
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s")  STORED AS PARQUET LOCATION '${rootPath}/${table}'"

      hiveSqlMap.put(table.toString, sql)
    })

    hiveSqlMap

  }


  def getSchema(): java.util.HashMap[String, StructType] = {
    val mapStructType= new java.util.HashMap[String, StructType]
    //对表进行遍历构造
    for (table <- tables) {
      val arrayStructFields = new ArrayBuffer[StructField]()
      val tableFields = config.getKeys(table.toString)
      //获取所有字段
      while (tableFields.hasNext) {
        val key = tableFields.next()                                      //带前缀的字段wechat.rksj
        val field = key.toString.split("\\.")(1)                  //真实字段  rksj
        val fieldType = config.getProperty(key.toString)                  //配置文件中的数据类型
        fieldType match {
          case "string" => arrayStructFields += StructField(field, StringType, true)
          case "long" => arrayStructFields += StructField(field, StringType, true)
          case "double" => arrayStructFields += StructField(field, StringType, true)
          case _ =>
        }

      }

      val schema = StructType(arrayStructFields)
      mapStructType.put(table.toString,schema)
    }
    mapStructType

  }

}
