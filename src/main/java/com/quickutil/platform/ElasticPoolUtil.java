package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * ES工具-配置池
 *
 * @author 0.5
 */
public class ElasticPoolUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ElasticPoolUtil.class);

	private Map<String, ElasticUtil> elasticUtilMap = new HashMap<>();

	public ElasticPoolUtil() {
	}

	public ElasticPoolUtil(Properties elasticProperties) {
		try {
			Enumeration<?> keys = elasticProperties.propertyNames();
			List<String> keyList = new ArrayList<String>();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				key = key.split("\\.")[0];
				if (!keyList.contains(key)) {
					keyList.add(key);
				}
			}
			for (String key : keyList) {
				String host = elasticProperties.getProperty(key + ".host");
				String username = elasticProperties.getProperty(key + ".username");
				String password = elasticProperties.getProperty(key + ".password");
				elasticUtilMap.put(key, new ElasticUtil(host, username, password));
			}
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
		}
	}

	public void add(String key, String host, String username, String password) {
		elasticUtilMap.put(key, new ElasticUtil(host, username, password));
	}

	public ElasticUtil get(String key) {
		return elasticUtilMap.get(key);
	}

	public void remove(String key) {
		elasticUtilMap.remove(key);
	}
}