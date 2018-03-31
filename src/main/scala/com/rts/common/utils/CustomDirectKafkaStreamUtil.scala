package com.rts.common.utils

import com.rts.dao.zookeeper.ZookeeperOffsetsDAO
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag


object CustomDirectKafkaStreamUtil {

  def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  (ssc: StreamingContext, kafkaParams: Map[String, String], zkOffsetsDAOImpl: ZookeeperOffsetsDAO, topic: String): InputDStream[(K, V)] = {

    // 读取offset
    val storedOffsets = zkOffsetsDAOImpl.readOffsets(topic)

    val kafkaStream = storedOffsets match {
      case None =>
        // 从最新的offsets开始读取
        KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, Set(topic))
      case Some(fromOffsets) =>
        // 从已经保存的offsets后一位开始读取
        val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    kafkaStream

  }

}
