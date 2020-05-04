package com.example.kafka.partitions.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class CountryPartitioner implements Partitioner {
	
	private LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();

	@Override
	public void configure(Map<String, ?> configs) {
		for(Map.Entry<String,?> entry: configs.entrySet()){
			if(entry.getKey().startsWith("partitions.")) {
				map.put(entry.getKey().substring(11), (Integer) entry.getValue());
			}
		}
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		if(key != null && map.containsKey(key)) {
			return map.get(key);
		}
		return 0;
	}

	@Override
	public void close() {
		
	}

}
