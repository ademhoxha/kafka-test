package com.example.kafka.partitions.util;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerFactory {

	public static Consumer<String, String> createConsumer(Boolean fromHistory, String id, int partitionId) {
		Properties p = new Properties();
		p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.HOST);
		p.put(ConsumerConfig.GROUP_ID_CONFIG, id);
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromHistory ? "earliest" : "latest");

		Consumer<String, String> consumer = new KafkaConsumer<>(p);
		// consumer.subscribe(Collections.singletonList(Configuration.TOPIC));
		consumer.assign(Collections.singletonList(new TopicPartition(Configuration.TOPIC, partitionId)));
		return consumer;

	}

	public static Consumer<String, String> createConsumer(Boolean fromHistory, int partitionId) {
		return createConsumer(fromHistory, Configuration.CONSUMER_GROUP_ID, partitionId);
	}

}