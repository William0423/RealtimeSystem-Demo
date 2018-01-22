package com.rts.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.rts.kafka.producer.KafkaProducer;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */

public class JavaDirectKafkaWordCount {
	
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args){
		
		// 直接执行main方法，写死下面参数
//		String brokers = args[0];
//		String topics = args[1];
		
		String brokers = "127.0.0.1:9092";
		String topics = KafkaProducer.TOPIC_NAME;
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[*]");
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		
	    // Create direct kafka stream with brokers and topics
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        String.class,
	        StringDecoder.class,
	        StringDecoder.class,
	        kafkaParams,
	        topicsSet
	    );
		

	    // Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(Tuple2::_2);
//	    lines.foreachRDD(rdd -> {
//	    	System.out.println();
//	    });
	    
	    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s,1)).reduceByKey((i1,i2) -> i1+i2);
	    wordCounts.print();
	    // Start the computation
	    jssc.start();
	    try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	    
	    
	}

}
