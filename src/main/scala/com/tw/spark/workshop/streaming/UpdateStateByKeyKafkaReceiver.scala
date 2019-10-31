package com.tw.spark.workshop.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//有状态数据统计
object UpdateStateByKeyKafkaReceiver {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("stream word count").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //设置保存数据状态的检查点目录
    ssc.checkpoint("checkpoint")


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

    val wordDStream: DStream[String] = kafkaDStream.flatMap(t => t.value().split(" "))

    val mapDStream: DStream[(String, Int)] = wordDStream.map(word => (word, 1))


    //将转换结构后的数据进行聚合
    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }


    stateDStream.print()


    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()

  }

}
