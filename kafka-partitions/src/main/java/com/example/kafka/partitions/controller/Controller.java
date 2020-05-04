package com.example.kafka.partitions.controller;

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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.partitions.util.Configuration;
import com.example.kafka.partitions.util.ConsumerFactory;
import com.example.kafka.partitions.util.ProducerFactory;
import com.example.kafka.partitions.util.ProducerRequest;


@RestController
public class Controller {
	
	@GetMapping("/version")
	public String version() {
		return "Version 0.1";
	}
	
	@PostMapping(value = "/produce")
	public String produce(@RequestBody() ProducerRequest body) throws InterruptedException, ExecutionException {
		Producer<String, String> producer = ProducerFactory.createProducer();
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(Configuration.TOPIC, body.getCountry(), body.getProduct());
		producer.send(record).get();
		producer.close();
		return "Produced " + body.getProduct();
	}
	
	@GetMapping("/consume/USA")
	public ArrayList<String> consumeUSA() {
		return this.consume(1);
	}
	
	@GetMapping("/consume/ITA")
	public ArrayList<String> consumeITA() {
		return this.consume(2);
	}
	
	@GetMapping("/consume/CHN")
	public ArrayList<String> consumeCHN() {
		return this.consume(3);
	}
	
	@GetMapping("/consume")
	public ArrayList<String> consumeNone() {
		return this.consume(0);
	}
	
	
	private ArrayList<String> consume(int id) {
		Date date = new Date();
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String stringDate = sdf.format(date);

		Consumer<String, String> historyConsumer = ConsumerFactory.createConsumer(Boolean.TRUE, stringDate, id);
		ArrayList<String> list = new ArrayList<String>();
		while (true) {
			ConsumerRecords<String, String> consumerRecords = historyConsumer.poll(Duration.ofSeconds(10));
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
