package com.atguigu.member.controller

import com.atguigu.member.bean.DwsMember
import com.atguigu.member.service.DwsMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * JOIN优化：
 * 广播：2.1.1之前只能用于spark table。需要broadcast（表名）
 *     之后版本只要设置.set("spark.sql.autoBroadcastJoinThreshold","104857600") //100M，100M可调整，就可以实现广播join
 *     spark的join方式，sortmergejoin，broadcastjoin，shuffleJoin（基本不用）
 *
 *内存优化：将java序列化更换成kryo序列化，设置序列化缓存级别
 *     .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
 *     .registerKryoClasses(Array(classOf[DwsMember]))
 *
 */

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    //给root用一个用户权限
    System.setProperty("HADOOP_USER_NAME","root")
    val sparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[4]")
//     .set("spark.sql.autoBroadcastJoinThreshold","104857600") //100M
//     .set("spark.sql.autoBroadcastJoinThreshold","-1") //100M
//

//     .set("spark.sql.shuffle.partitions","12")
//     .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//     .registerKryoClasses(Array(classOf[DwsMember]))
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val ssc = sparkSession.sparkContext
    //HA（高可用）
    //默认文件系统
    ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
    //命名空间
    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")

    //开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    //开启压缩
    HiveUtil.openCompression(sparkSession)
    //根据用户信息聚合用户表数据
//    DwsMemberService.importMember(sparkSession,"20190722")
    DwsMemberService.importMemberUseApi(sparkSession,"20190722")
  }
}
