package com.atguigu.qzpoint.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    //设置10个分区为了和kafka中的分区数对应1:1的关系，这样发送速度更快
//    ssc.textFile("/user/atguigu/kafka/register.log",10)
    ssc.textFile("file://"+this.getClass.getResource("/register.log").getPath, 10)
      .foreachPartition(partition => {
        val props = new Properties()
        //kafka节点
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        //ack（0,1，-1）根据生产需求设置对应参数
        props.put("acks", "1")
        //producer批量发送的基本单位，默认是16384Bytes
        props.put("batch.size", "16384")
        //lingger.ms是sender线程在检查batch是否ready时候，判断有没有过期的参数，默认大小是0ms
        //满足batch.size和ling.ms之一，producer便开始发送消息
        props.put("linger.ms", "10")
/*        Kafka的客户端发送数据到服务器，一般都是要经过缓冲的，也就是说，
        你通过KafkaProducer发送出去的消息都是先进入到客户端本地的内存缓冲里，
        然后把很多消息收集成一个一个的Batch，再发送到Broker上去的。*/
        props.put("buffer.memory", "33554432")
        //指定序列化，否则报错
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("register_topic",item)
          //异步发送
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
