package com.rts.spark.checkpoint

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * kafka 0.10版本的checkpoint实现
  */
object ExactlyOnce010 {
  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      System.err.println("Userage: ExactlyOnce <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stremingtopic_group",
      // kafka autocommit can happen before batch is finished, turn it off in favor of checkpoint only
      "enable.auto.commit" -> (false: java.lang.Boolean),
      // start from the smallest available offset, ie the beginning of the kafka log
      "auto.offset.reset" -> "earliest"
    )

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

  def setupSsc(topics: Set[String], kafkaParams: Map[String, Object],  checkpointDir: String)(): StreamingContext = {
    //    val ssc = new StreamingContext(new SparkConf, Seconds(5)) // 线上的环境
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ExactlyOnce010")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
        stream.foreachRDD { rdd =>
          rdd.foreachPartition { iter =>
            // make sure connection pool is set up on the executor before writing
            //        SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)
            iter.foreach { record: ConsumerRecord[String, String] =>
              println("###############")
              println(record.value())
              //          DB.autoCommit { implicit session =>
              //            // the unique key for idempotency is just the text of the message itself, for example purposes
              //            sql"insert into idem_data(msg) values (${record.value()})".update.apply
              //          }
            }
          }
        }

    // the offset ranges for the stream will be stored in the checkpoint
    ssc.checkpoint(checkpointDir)
    //设置通过间隔时间，定时持久checkpoint到hdfs上：官网推荐————刷新间隔一般为批处理间隔的5到10倍是比较好的一个方式
    //    stream.checkpoint(Seconds(batchDuration*5))
    ssc
  }
}
