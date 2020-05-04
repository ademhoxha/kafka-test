package com.example.kafka;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import util.Config;
import util.ConsumerFactory;
import util.ProducerFactory;

@RestController
public class Controller {

	// private Producer<Long, String> producer = ProducerFactory.createProducer();
	// private Consumer<Long, String> realtimeConsumer =
	// ConsumerFactory.createConsumer(Boolean.FALSE);
	// private Consumer<Long, String> historyConsumer =
	// ConsumerFactory.createConsumer(Boolean.FALSE);

	@GetMapping("/version")
	public String version() {
		return "Version 1.2";
	}

	@GetMapping("/produce")
	public String produce(@RequestParam() String msg) throws InterruptedException, ExecutionException {
		Producer<Long, String> producer = ProducerFactory.createProducer();
		ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Config.TOPIC, msg);
		producer.send(record).get();
		producer.close();
		return "Produced " + msg;
	}

	@GetMapping("/consume")
	public ArrayList<String> consume() {

		Date date = new Date();
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String stringDate = sdf.format(date);

		Consumer<Long, String> historyConsumer = ConsumerFactory.createConsumer(Boolean.TRUE, stringDate);
		ArrayList<String> list = new ArrayList<String>();
		while (true) {
			ConsumerRecords<Long, String> consumerRecords = historyConsumer.poll(Duration.ofSeconds(10));
			if (consumerRecords.count() == 0) {
				break;
			}
			consumerRecords.forEach(record -> {
				list.add(record.value());
			});
			historyConsumer.commitSync();
		}
		historyConsumer.close();
		return list;
	}

	@GetMapping("/consume/static")
	public ArrayList<String> consumeStatic() {
		Consumer<Long, String> historyConsumer = ConsumerFactory.createConsumer(Boolean.FALSE);
		ArrayList<String> list = new ArrayList<String>();
		while (true) {
			ConsumerRecords<Long, String> consumerRecords = historyConsumer.poll(Duration.ofSeconds(10));
			if (consumerRecords.count() == 0) {
				break;
			}
			consumerRecords.forEach(record -> {
				list.add(record.value());
			});
			historyConsumer.commitSync();
		}
		historyConsumer.close();
		return list;
	}
}
