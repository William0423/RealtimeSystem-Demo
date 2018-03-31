package com.rts.spark.exactlyonce.zookeeper


import com.rts.common.utils.CustomDirectKafkaStreamUtil
import com.rts.dao.zookeeper.ZookeeperOffsetsDAO
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory


object RealtimeSystemDemoApp extends LazyLogging {

  def main(args: Array[String]): Unit = {

    // main方法传入的参数格式：host1:9092,host2:9092,host3:9092 topic1,topic2
    if(args.length != 2){
      System.err.println("Userage: RealtimeSystemDemoApp <brokers> <topics>")
      System.exit(1)
    }

    // 加载工程resources目录下application.conf文件
    val applicationConf = ConfigFactory.load

    val sparkConf = new SparkConf().setAppName("RealtimeSystemDemoApp").setMaster("local[2]")
//    val sparkConf = new SparkConf()
    // 设置每次读取kafka的数据条目
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", applicationConf.getString("streaming.kafka_maxRatePerPartition"))
    // 初始化StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(applicationConf.getLong("streaming.batch_intervalTime")))

    // 根据sparkConf创建session，并获取sparkContext
    sparkConf.set("spark.mongodb.input.uri", applicationConf.getString("mongoDB_connection_uri"))
    sparkConf.set("spark.mongodb.output.uri", applicationConf.getString("mongoDB_connection_uri"))
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    // val sc = session.sparkContext // 从session中获取sparkContext


    val Array(brokers, topicStr) = args
    val topicsSet = topicStr.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers,
      "auto.offset.reset" -> "smallest")
    // 传入的两个参数分别为：ip:host及存储在zookeeper上的offsets对应路径
    val zkOffsetsDAOImpl = new ZookeeperOffsetsDAO(applicationConf.getString("zookeeper.hosts"), applicationConf.getString("zookeeper.offsets_path"))

    for (topic <- topicsSet) {
      val messagesDStream = CustomDirectKafkaStreamUtil.kafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, zkOffsetsDAOImpl, topic)
      val xmlDataRDD = messagesDStream.map(_._2).flatMap(_.split("\n"))
      xmlDataRDD.foreachRDD { rdd =>
        for (line <- rdd.collect().toArray) {
          println("############---------print line result from kafka:---------############")
          println(line)
        }
      } // 遍历RDD结束

      // 已经保存到数据库的kafka偏移量offsets，存到zk中
      messagesDStream.foreachRDD(rdd => zkOffsetsDAOImpl.saveOffsets(topic, rdd))
    }

    ssc.start()
    ssc.awaitTermination()
  }




}

