import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object WebPagePopularityValueCalculator {
  private val processingInterval:Int=5
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group1"

  private val WeightNumClick: Double = 0.8
  private val WeightTimeStay: Double = 0.8

  //  f(x,y,z)=0.8x+0.8y+z
  //  (page001.html, 1, 0.5, 1)，利用公式可得：
  //  H(page001)=f(x,y,z)= 0.8x+0.8y+z=0.8*1+0.8*0.5+1*1=2.2
  def computeWebPopularityIndex(numClick: Float, timeOnPage: Float, likeOrNot: Float): Double = {
    WeightNumClick * numClick + WeightTimeStay * timeOnPage + likeOrNot
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Web Page Popularity Value Calculator").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(processingInterval))

    //直连方式相当于跟kafka的Topic至直接连接
    //"auto.offset.reset:earliest(每次重启重新开始消费)，latest(重启时会从最新的offset开始读取)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fwmagic",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("tw-workshop")

    //using updateStateByKey asks for enabling checkpoint
    ssc.checkpoint(checkpointDir)


    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val msgDataRDD = kafkaDStream.map(_.value())

    //for debug use only
    //println("Coming data in this interval...")
    //msgDataRDD.print()
    // e.g page37|5|1.5119122|-1
    msgDataRDD.print()
    //    kafkaDStream.map(record => (record.key, record.value))

    val popularityData = msgDataRDD.map { msgLine => {
      val dataArr: Array[String] = msgLine.split("\\|")
      val pageID = dataArr(0)
      val numClick = dataArr(1).toFloat
      val timeOnPage = dataArr(2).toFloat
      val likeOrNot = dataArr(3).toFloat
      //calculate the popularity value
      val popValue: Double = computeWebPopularityIndex(numClick, timeOnPage, likeOrNot)
      (pageID, popValue)
    }
    }

    //sum the previous popularity value and current value
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }
    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    stateDstream.checkpoint(Duration(8 * processingInterval.toInt * 1000))
    //after calculation, we need to sort the result and only show the top 10 hot pages
    stateDstream.foreachRDD { rdd => {
      val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
      val topKData = sortedData.take(5).map { case (v, k) => (k, v) }
      topKData.foreach(x => {
        println(x)
      })
    }
    }


    ssc.start()
    ssc.awaitTermination()

  }

}