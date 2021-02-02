package com.mycompany;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ProblemDemoJob {
	private static final String TOPICNAME = "test";

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(5000L);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS);
		env.getCheckpointConfig().setCheckpointTimeout(CheckpointConfig.DEFAULT_TIMEOUT);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(CheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS);

		Properties properties = buildKafkaProperties();
		DataStream<String> dataStream = env
				.addSource(new FlinkKafkaConsumer011<>(TOPICNAME, new SimpleStringSchema(), properties));

		dataStream.map(e -> {
			System.out.println("received element " + e);
			return e;
		});

		dataStream.print();

		env.execute("ProblemDemoJob");
	}

	private static Properties buildKafkaProperties() {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroupprobtest");
		return properties;
	}

}
