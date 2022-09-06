package com.trans.test.transformautomation;
;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.io.InputStream;
import java.util.Properties;


public class TransformAutomationApplication {

	public static void main(String[] args) throws Exception{

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AutoCloud");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> source = builder.stream("raw_product");
		String resourceName = "/Rules.json";
		InputStream is = TransformAutomationApplication.class.getResourceAsStream(resourceName);
		if (is == null) {
			throw new NullPointerException("Cannot find resource file " + resourceName);
		}
		JSONTokener tokener = new JSONTokener(is);
		JSONObject object = new JSONObject(tokener);
		String columnName= object.getString("columname");
		String columnRule= object.getString("check");

		KStream<String, String> target = source.map((x,y)->{
			JSONObject jsonObj = new JSONObject(y);
			if(jsonObj.get(columnName).equals(columnRule)){
				jsonObj.put("IsDataValid", "N");
				jsonObj.put("Country","USA");
			} else {
				jsonObj.put("IsDataValid", "Y");
				jsonObj.put("Country","USA");
			}
			return new KeyValue(x, jsonObj.toString());
		});
		target.foreach((x,y)->{
			System.out.println("Value:"+y);
		});

		target.to("trusted_product");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
