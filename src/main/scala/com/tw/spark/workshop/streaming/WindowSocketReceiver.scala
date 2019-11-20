package com.tw.spark.workshop.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//有状态数据统计
object WindowSocketReceiver {


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

    val socketLineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //创建window,注意窗口大小、步长和采集周期的关系
    val windowDStream: DStream[String] = socketLineDStream.window(Seconds(10), Seconds(5))


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
