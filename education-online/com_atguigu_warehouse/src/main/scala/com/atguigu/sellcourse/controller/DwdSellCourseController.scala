package com.atguigu.sellcourse.controller

import com.atguigu.sellcourse.service.DwdSellCourseService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdSellCourseController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_sellcourse_import")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    DwdSellCourseService.importCoursePay(ssc,sparkSession)
    DwdSellCourseService.importCourseShoppingCart(ssc,sparkSession)
    DwdSellCourseService.importSaleCourseLong(ssc,sparkSession)

  }
}
