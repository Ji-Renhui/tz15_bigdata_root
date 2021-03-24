package tz15_bigdata_root.spark.hive

import org.apache.hadoop.fs.{FileStatus, FileUtil, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import tz15_bigdata_root.spark.common.SessionFactory
import scala.collection.JavaConversions._
/**
 * @ClassName CombineHdfs
 * @Description小文件合并
 * @Date 2021/2/25 11:27
 * @Created by robot
 **/
object CombineHdfs extends Serializable with Logging{
  def main(args: Array[String]): Unit = {


    //-----------------------------------删除小文件-------------------------------------------------------------
    val sparkSession = SessionFactory.newLocalHiveSession("CombineHdfs", 2)
    //读取HDFS文件
    val tables: java.util.List[_] = List("wechat","qq","mail")
        tables.foreach(table => {
      println("===============开始进行小文件合并=================")
      val table_Path = s"hdfs://mycluster${HiveConfig.rootPath}/${table}"
      val tableDF = sparkSession.read.load(table_Path)

      //------在大文件写入之前获取小文件路径，避免删除掉新写入的大文件-------------------------------------
      val fileSystem = HdfsAdmin.get().getFs
      val fileStatusesArray: Array[FileStatus] = fileSystem.globStatus(new Path(table_Path + "/part*"))
      //将状态转为文件路径
      val paths = FileUtil.stat2Paths(fileStatusesArray)
      //-------在大文件写入之前获取小文件路径，避免删除掉新写入的大文件------------------------------------

      //合并小文件，并写回原路径
      tableDF.repartition(1).write.mode(SaveMode.Append).parquet(table_Path)
      //删除原来的小文件

      paths.foreach(paths => {
        fileSystem.delete(paths)
        println(s"==========删除小文件${paths}成功============")
      })
    })
    //-----------------------------------删除小文件-----------------------------------------------------------------------

  }
}
