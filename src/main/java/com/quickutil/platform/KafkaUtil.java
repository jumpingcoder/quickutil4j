package com.quickutil.platform;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

/**
 * Kafka客户端工具
 *
 * @author hao.hu
 */
public class KafkaUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(KafkaUtil.class);

	private static Map<String, KafkaProducer<String, String>> kafkaProducerMap = new HashMap<>();
	private static Map<String, KafkaConsumer<String, String>> kafkaConsumerMap = new HashMap<>();
	private static Map<String, Properties> kafkaProducerProperties = new HashMap<>();
	private static Map<String, Properties> kafkaConsumerProperties = new HashMap<>();

	public static boolean addKafkaProducer(Properties producer) {
		try {
			List<String> keyList = new ArrayList<String>();
			Enumeration<?> keys = producer.propertyNames();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				key = key.split("\\.")[0];
				if (!keyList.contains(key)) {
					keyList.add(key);
				}
			}
			for (String key : keyList) {
				Properties oneProperty = new Properties();
				Enumeration<?> sourcekeys = producer.propertyNames();
				while (sourcekeys.hasMoreElements()) {
					String sourcekey = (String) sourcekeys.nextElement();
					String first = sourcekey.split("\\.")[0];
					if (key.equals(first)) {
						oneProperty.setProperty(sourcekey.substring(key.length() + 1), producer.getProperty(sourcekey));
					}
				}
				kafkaProducerProperties.put(key, oneProperty);
				kafkaProducerMap.put(key, buildKafkaProducer(oneProperty));
				LOGGER.info("KafkaProducer init success -- kafkaProvider: {}", key);
			}
			return true;
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return false;
	}

	private static KafkaProducer<String, String> buildKafkaProducer(Properties oneProperty) {
		return new KafkaProducer<String, String>(oneProperty);
	}

	public static KafkaProducer<String, String> getKafkaProducer(String producer) {
		return kafkaProducerMap.get(producer);
	}

	public static String getKafkaProducerProperty(String producer, String key) {
		return kafkaProducerProperties.get(producer).getProperty(key);
	}

	public static boolean addKafkaConsumer(Properties consumer) {
		try {
			List<String> keyList = new ArrayList<String>();
			Enumeration<?> keys = consumer.propertyNames();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				key = key.split("\\.")[0];
				if (!keyList.contains(key)) {
					keyList.add(key);
				}
			}
			for (String key : keyList) {
				Properties oneProperty = new Properties();
				Enumeration<?> sourcekeys = consumer.propertyNames();
				while (sourcekeys.hasMoreElements()) {
					String sourcekey = (String) sourcekeys.nextElement();
					String first = sourcekey.split("\\.")[0];
					if (key.equals(first)) {
						oneProperty.setProperty(sourcekey.substring(key.length() + 1), consumer.getProperty(sourcekey));
					}
				}
				kafkaConsumerProperties.put(key, oneProperty);
				kafkaConsumerMap.put(key, buildKafkaConsumer(oneProperty));
			}
			return true;
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return false;
	}

	private static KafkaConsumer<String, String> buildKafkaConsumer(Properties oneProperty) {
		return new KafkaConsumer<String, String>(oneProperty);
	}

	public static KafkaConsumer<String, String> getKafkaConsumer(String consumer) {
		return kafkaConsumerMap.get(consumer);
	}

	public static String getKafkaConsumerProperty(String consumer, String key) {
		return kafkaConsumerProperties.get(key).getProperty(key);
	}
}
