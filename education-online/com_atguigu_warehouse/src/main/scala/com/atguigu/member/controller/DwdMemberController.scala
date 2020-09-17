package com.atguigu.member.controller

import com.atguigu.member.service.EtlDataService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DwdMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")

    //开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    //开启压缩
    HiveUtil.openCompression(sparkSession)
    //开启Snapppy压缩,spark自带snappy压缩
//  HiveUtil.useSnapppyCompression(sparkSession)

    //导入基础广告表数据
    EtlDataService.etlBaseAdLog(ssc,sparkSession)
    //导入基础网站表数据
    EtlDataService.etlBaseWebSiteLog(ssc, sparkSession)
    //清洗用户数据
    EtlDataService.etlMemberLog(ssc, sparkSession)
    //清洗用户注册数据
    EtlDataService.etlMemberRegtypeLog(ssc, sparkSession)
    //导入用户支付情况记录
    EtlDataService.etlMemPayMoneyLog(ssc, sparkSession)
    //导入vip基础数据
    EtlDataService.etlMemVipLevelLog(ssc, sparkSession)
  }
}
