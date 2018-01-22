package com.rts.kafka.producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka消息生产者
 */
public class KafkaProducer {
	private final Producer<String, String> producer;
	public final static String TOPIC_NAME = "poetry";
	public final static String TOPIC_GROUP_ID = "poetry-group";

	private KafkaProducer() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		props.put("metadata.broker.list", "192.168.81.87:9092");
		props.put("zk.connect", "192.168.81.87:2181");
		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "-1");
		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	/**
	 * 生产信息
	 */
	void produce() {
		int no = 10;
		final int COUNT = 100;

		while (no < COUNT) {
			Map<String, Object> map = new HashMap<String, Object>();
			String key = String.valueOf(no);
			String cNum = "idc-cNum-" + no;
			String portid = "port-" + no;
			Double data = 12.25;
			map.put("cNum", cNum);
			map.put("portid", portid);
			map.put("data", data);
			producer.send((List<KeyedMessage<String, String>>) new KeyedMessage<String, Map<String, Object>>(TOPIC_NAME, key, map));
			no++;
		}
	}

	public static void main(String[] args) {
		new KafkaProducer().produce();
	}
}
