package com.rts.dao.zookeeper

import com.rts.common.utils.StopwatchUtil
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges


class ZookeeperOffsetsDAO(zkHosts: String, zkPath: String) extends LazyLogging {

  // 会话超时：10000； 连接超时：10000
  private val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)

  /**
    * 读取保存到zookeeper上最新的offset
    * @param topic
    * @return
    */
   def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]] = {
    logger.info("Reading offsets from ZooKeeper")
    val stopwatch = new StopwatchUtil()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.debug(s"Read offset ranges: ${offsetsRangesStr}")
        // offsetsRangesStr结果： 0:0,5:288,1:0,2:0,4:575,3:0
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap
        // offsets结果：Map([stress_testing_topic,0] -> 0, [stress_testing_topic,5] -> 288, [stress_testing_topic,1] -> 0, [stress_testing_topic,2] -> 0, [stress_testing_topic,4] -> 575, [stress_testing_topic,3] -> 0)
        logger.info("Done reading offsets from ZooKeeper. Took " + stopwatch)
        Some(offsets)
      case None =>
        logger.info("No offsets found in ZooKeeper. Took " + stopwatch)
        None
    }

  }


  /**
    * 保存kafka的offsets
    * @param topic
    * @param rdd
    */
   def saveOffsets(topic: String, rdd: RDD[_]): Unit = {
    logger.info("Saving offsets to ZooKeeper")
    val stopwatch = new StopwatchUtil()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.debug(s"Writing offsets to ZooKeeper: ${offsetsRangesStr}")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    logger.info("Done updating offsets in ZooKeeper. Took " + stopwatch)
  }


}
