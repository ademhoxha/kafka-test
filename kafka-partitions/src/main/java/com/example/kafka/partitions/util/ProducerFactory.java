package com.example.kafka.partitions.util;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerFactory {

	public static KafkaProducer<String, String> createProducer() {
		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.HOST);
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
		p.put("partitions.USA", 1);
		p.put("partitions.ITA", 2);
		p.put("partitions.CHN", 3);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getName());
		return new KafkaProducer<String, String>(p);
	}
}
