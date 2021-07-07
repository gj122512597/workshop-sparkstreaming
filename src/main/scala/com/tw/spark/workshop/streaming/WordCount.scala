package com.tw.spark.workshop.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("mywordcount").setMaster("local[*]")

    //    构建 Streaming Context 对象StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //    创建 InputDStream
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9998)

    //    操作 Dstream
    val wordsDS: DStream[String] = socketDS.flatMap(_.split(" "))
    val wordKvDS: DStream[(String, Int)] = wordsDS.map(word => {
      (word, 1)
    })
    val result: DStream[(String, Int)] = wordKvDS.reduceByKey(_+_)
    result.print()

    //    启动采集器
    ssc.start()

    //    Driver等待采集器的执行
    ssc.awaitTermination()

  }

}
