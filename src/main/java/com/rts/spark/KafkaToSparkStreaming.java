package com.rts.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.rts.kafka.producer.KafkaProducer;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


/**
 * 对应链接：https://stackoverflow.com/questions/40926947/how-to-convert-javapairinputdstream-into-dataset-dataframe-in-spark
 * 构建一个Spark Streaming应用程序的四个步骤：https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-streaming/index.html
 * 1、构建Steaming Context对象；
 * 2、创建InputDStream:Spark Streaming支持多种不同的数据源，比如kafkaStream、flumeStream、fileStream、networkStream 等
 * 3、操作DStream:
 * 4、启动spark streaming
 * @author hadoop
 *
 */
public class KafkaToSparkStreaming {
	   public static  void main(String arr[]) throws InterruptedException
	    {


	        SparkConf conf = new SparkConf();
	        conf.set("spark.app.name", "SparkReceiver"); //The name of application. This will appear in the UI and in log data.
	        //conf.set("spark.ui.port", "7077");    //Port for application's dashboard, which shows memory and workload data.
	        conf.set("dynamicAllocation.enabled","false");  //Which scales the number of executors registered with this application up and down based on the workload
	        //conf.set("spark.cassandra.connection.host", "localhost"); //Cassandra Host Adddress/IP
	        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");  //For serializing objects that will be sent over the network or need to be cached in serialized form.
	        conf.setMaster("local");
	        conf.set("spark.streaming.stopGracefullyOnShutdown", "true");

	        JavaSparkContext sc = new JavaSparkContext(conf);
	        // Create the context with 2 seconds batch size
	        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

	        Map<String, String> kafkaParams = new HashMap<String, String>();
//	        kafkaParams.put("zookeeper.connect", "localhost:2181"); //Make all kafka data for this cluster appear under a particular path. 
	        kafkaParams.put("zookeeper.connect", "192.168.81.87:2181"); //Make all kafka data for this cluster appear under a particular path. 
	        kafkaParams.put("group.id", KafkaProducer.TOPIC_GROUP_ID);   //String that uniquely identifies the group of consumer processes to which this consumer belongs
//	        kafkaParams.put("metadata.broker.list", "localhost:9092"); //Producer can find a one or more Brokers to determine the Leader for each topic.
	        kafkaParams.put("metadata.broker.list", "192.168.81.87:9092"); //Producer can find a one or more Brokers to determine the Leader for each topic.
	        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder"); //Serializer to use when preparing the message for transmission to the Broker.
	        kafkaParams.put("request.required.acks", "1");  //Producer to require an acknowledgement from the Broker that the message was received.

	        Set<String> topics = Collections.singleton(KafkaProducer.TOPIC_NAME);

	        //Create an input DStream for Receiving data from socket
	        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
	                String.class, 
	                String.class, 
	                StringDecoder.class, 
	                StringDecoder.class, 
	                kafkaParams, topics);

	        //Create JavaDStream<String>
	        JavaDStream<String> msgDataStream = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
	            @Override
	            public String call(Tuple2<String, String> tuple2) {
	              return tuple2._2();
	            }
	          });
	        msgDataStream.foreachRDD(x ->  {
	        	x.collect().stream().forEach(n->System.out.println("spark streaming producer: " + n));
	        });
	        
	        //Create JavaRDD<Row>
//		msgDataStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//			@Override
//			public void call(JavaRDD<String> rdd) {
//				JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
//					@Override
//					public Row call(String msg) {
//
//						Row row = RowFactory.create(msg);
//						return row;
//					}
//				});
//				// Create Schema
//				StructType schema = DataTypes.createStructType(
//						new StructField[] { DataTypes.createStructField("Message", DataTypes.StringType, true) });
//				// Get Spark 2.0 session
//				SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
//				Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
//				msgDataFrame.show();
//			}
//		});
	        
//	        List<String> list = new LinkedList<String>();
//	        list.add("first");
//	        list.add("second");
//	        list.add("third");
//	        JavaRDD<String> myVeryOwnRDD = sc.parallelize(list);
//	        Queue<JavaRDD<String>> queue = new LinkedList<JavaRDD<String>>();
//	        queue.add( myVeryOwnRDD );
//	        JavaDStream<String> javaDStream = ssc.queueStream( queue );
//	        javaDStream.foreachRDD( x-> {
//	            x.collect().stream().forEach(n-> System.out.println("item of list: "+n));
//	        });

	        ssc.start();            
	        ssc.awaitTermination();  
	    }
	
}
