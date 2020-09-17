package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.ResultSet

import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
 * 知识点，如何手动获取、提交偏移量
 */

object RegisterStreaming {
  //消费者组
  private val groupid = "register_group_test"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //控制消费速度的参数，意思是每个分区上每秒钟消费的条数
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
//      .set("spark.streaming.backpressure.enabled", "true")
//      .set("spark.streaming.stopGracefullyOnShutdown", "true") //设置优雅关闭
      .setMaster("local[*]")
    //第二个参数代表批次时间，即三秒一批数据
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext: SparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")//指定namenode群组
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    //用数组存放topic，意思就是我这里可以监控多个topic
    val topics = Array("register_topic")
    //注意Map里面的泛型必须是String，和Object
    val kafkaMap: Map[String, Object] = Map[String, Object](
      //kafka监控地址
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      //指定kafka反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //消费者组
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",   //sparkstreaming第一次启动，不丢数
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")

    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        //第一个参数是存放一个StreamingContext
        //第二个参数是消费数据平衡策略，这里用的是均匀消费
        //第三个参数是具体要监控的对象（里面包含topic，kafkaMap，偏移量）
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    //stream原始流无法进行使用和打印，会报序列化错误，所以需要做下面的map转换
    val resultDStream = stream.filter(item => item.value().split("\t").length == 3).
      mapPartitions(partitions => {
        partitions.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val app_name = arr(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })
    resultDStream.cache()
    //(PC,1),(PC,1),(APP,1),(Other,1),(APP,1),(Other,1),(PC,1),(APP,1)
    //"=================每6s间隔1分钟内的注册数据================="
    //滑动窗口，注意窗口大小和滑动步长必须是批次时间的整数倍
    // 第一个参数是具体要做什么操作
    // 第二个参数是窗口大小
    // 第三个参数是滑动步长
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()
    //"========================================================="

    //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"//状态计算
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum //本批次求和
      val previousCount = state.getOrElse(0) //历史数据
      Some(currentCount + previousCount)
    }
    //updateStateByKey算子会去跟历史状态数据做一个计算，所以要提前设置一个历史状态保存路径
    resultDStream.updateStateByKey(updateFunc).print()

    //"++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    //数据倾斜解决方式如下：两阶段聚合

    //    val dsStream = stream.filter(item => item.value().split("\t").length == 3)
    //      .mapPartitions(partitions =>
    //        partitions.map(item => {
    //          val rand = new Random()
    //          val line = item.value()
    //          val arr = line.split("\t")
    //          val app_id = arr(1)
    //          (rand.nextInt(3) + "_" + app_id, 1)
    //        }))
    //    val result = dsStream.reduceByKey(_ + _)
    //    result.map(item => {
    //      val appid = item._1.split("_")(1)
    //      (appid, item._2)
    //    }).reduceByKey(_ + _).print()

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
