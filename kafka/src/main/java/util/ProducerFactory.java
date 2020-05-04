package util;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerFactory {

	public static KafkaProducer<Long,String> createProducer() {
		Properties p = new  Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG ,  "localhost:9092");
		p.put(ProducerConfig.CLIENT_ID_CONFIG , "producerId");
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ,  LongSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
		return new KafkaProducer<Long,String>(p);
	}
	
}
