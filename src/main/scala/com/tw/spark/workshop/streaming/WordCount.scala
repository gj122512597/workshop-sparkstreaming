package com.tw.spark.workshop.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("stream word count").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))


    val socketLineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = socketLineDStream.flatMap(_.split(" "))

    val mapDStream: DStream[(String, Int)] = wordDStream.map(word => (word, 1))

    val wordToSumDSTream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    wordToSumDSTream.print()


//    wordToSumDSTream.foreachRDD(rdd => {
//
//
//      //      println("driver  *********** ************")
//
//      rdd.foreach(d => {
//
//        //        println("rdd foreach $$$$$$$$$$$$$$$$$$$$$$$$$$$")
//        println(d._1 + "count:" + d._2)
//      })
//    })

    //启动采集器
    ssc.start()
    //Driver等待采集器的执行
    ssc.awaitTermination()

    //can't be called
    ssc.stop()
  }

}
