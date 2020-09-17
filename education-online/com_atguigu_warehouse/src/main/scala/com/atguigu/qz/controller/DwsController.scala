package com.atguigu.qz.controller

import com.atguigu.qz.service.DwsQzService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("dws_qz_controller").set("spark.sql.shuffle.partitions", "60")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    val dt = "20190722"
    DwsQzService.saveDwsQzChapter(sparkSession, dt)
    DwsQzService.saveDwsQzCourse(sparkSession, dt)
    DwsQzService.saveDwsQzMajor(sparkSession, dt)
    DwsQzService.saveDwsQzPaper(sparkSession, dt)
    DwsQzService.saveDwsQzQuestionTpe(sparkSession, dt)
    DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)

  }
}
