package com.tw.spark.workshop.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//有状态数据统计
object WindowKafkaReceiver {


  def main(args: Array[String]): Unit = {


    //Scala的窗口操作
    val ints = List(1, 2, 3, 4, 5, 6)

    //滑动
    val intses: Iterator[List[Int]] = ints.sliding(2, 2)

    for (list <- intses) {
      println(list.mkString(","))
    }

    val conf = new SparkConf().setAppName("stream word count").setMaster("local[*]")
    //
    val ssc = new StreamingContext(conf, Seconds(5))


    val bootStrapServers: String = "localhost:9092"
    val topics = Array("tw-workshop")


    //从Kafka中采集数据
    //直连方式相当于跟kafka的Topic至直接连接
    //"auto.offset.reset:earliest(每次重启重新开始消费)，latest(重启时会从最新的offset开始读取)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootStrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "thoughtwork001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )


    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val kafkaStringDStream: DStream[String] = kafkaDStream.map(t => {
      t.value()
    })


    //创建window,注意窗口大小、步长和采集周期的关系
    val windowDStream: DStream[String] = kafkaStringDStream.window(Seconds(10), Seconds(5))


    val wordDStream: DStream[String] = windowDStream.flatMap(t => t.split(" "))

    val mapDStream: DStream[(String, Int)] = wordDStream.map(word => (word, 1))

    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)


    wordToSumDStream.print()


    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()

  }

}
