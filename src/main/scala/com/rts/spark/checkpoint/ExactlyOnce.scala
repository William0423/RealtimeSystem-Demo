package com.rts.spark.checkpoint

import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 0.8版本的spark streaming和kafka依赖包
  */
object ExactlyOnce {
  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      System.err.println("Userage: ExactlyOnce <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers,
      "auto.offset.reset" -> "smallest") // 旧版本没有earliest，只有smallest和latest

    // while the job doesn't strictly need checkpointing,
    // we'll checkpoint to avoid replaying the whole kafka log in case of failure
    val checkpointDir = "/spark_kafka/offset/checkpoint"

    val ssc = StreamingContext.getOrCreate(
      checkpointDir,
      setupSsc(topicsSet, kafkaParams, checkpointDir) _
    )
    ssc.start()
    ssc.awaitTermination()
  }

  def setupSsc(topics: Set[String], kafkaParams: Map[String, String],  checkpointDir: String)(): StreamingContext = {
    //    val ssc = new StreamingContext(new SparkConf, Seconds(5)) // 线上的环境
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ExactlyOnce")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    /*旧版本*/
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val lines = messages.map(_._2).flatMap(_.split("\n"))
    lines.foreachRDD(rdd => (
      //      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (line <- rdd.collect().toArray){
        println("############")
        println(line)

      }
      ))
    // the offset ranges for the stream will be stored in the checkpoint
    ssc.checkpoint(checkpointDir)
    //设置通过间隔时间，定时持久checkpoint到hdfs上：官网推荐————刷新间隔一般为批处理间隔的5到10倍是比较好的一个方式
    //    stream.checkpoint(Seconds(batchDuration*5))
    ssc
  }
}
