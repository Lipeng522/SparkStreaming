package com.atguigu.qz.controller

import com.atguigu.qz.service.EtlDataService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_doexercise_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

//    ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
//    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")
    //开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    //开启压缩
    HiveUtil.openCompression(sparkSession)
    //使用snappy压缩
    HiveUtil.useSnapppyCompression(sparkSession)

    EtlDataService.etlQzChapter(ssc, sparkSession)
    EtlDataService.etlQzChapterList(ssc, sparkSession)
    EtlDataService.etlQzPoint(ssc, sparkSession)
    EtlDataService.etlQzPointQuestion(ssc, sparkSession)
    EtlDataService.etlQzSiteCourse(ssc, sparkSession)
    EtlDataService.etlQzCourse(ssc, sparkSession)
    EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
    EtlDataService.etlQzWebsite(ssc, sparkSession)
    EtlDataService.etlQzMajor(ssc, sparkSession)
    EtlDataService.etlQzBusiness(ssc, sparkSession)
    EtlDataService.etlQzPaperView(ssc, sparkSession)
    EtlDataService.etlQzCenterPaper(ssc, sparkSession)
    EtlDataService.etlQzPaper(ssc, sparkSession)
    EtlDataService.etlQzCenter(ssc, sparkSession)
    EtlDataService.etlQzQuestion(ssc, sparkSession)
    EtlDataService.etlQzQuestionType(ssc, sparkSession)
    EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)

  }
}
