package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 关系型数据库Jdbc工具-配置池
 *
 * @author 0.5
 */
public class JdbcPoolUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JdbcPoolUtil.class);
	private Map<String, JdbcUtil> jdbcUtilMap = new HashMap<>();

	public JdbcPoolUtil() {

	}

	public JdbcPoolUtil(Properties jdbcProperties, Properties druidProperties) {
		Enumeration<?> keys = jdbcProperties.propertyNames();
		List<String> keyList = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			if (!keyList.contains(key)) {
				keyList.add(key);
			}
		}
		for (String dbName : keyList) {
			try {
				String url = jdbcProperties.getProperty(dbName + ".url");
				String username = jdbcProperties.getProperty(dbName + ".username");
				String password = jdbcProperties.getProperty(dbName + ".password");
				if (url == null || username == null || password == null)
					throw new MissingParametersException("init requires url, username, password");
				String initConNumStr = jdbcProperties.getProperty(dbName + ".initconnum");
				int initConNum = initConNumStr == null ? Runtime.getRuntime().availableProcessors() : Integer.parseInt(initConNumStr);
				String minConNumStr = jdbcProperties.getProperty(dbName + ".minconnum");
				int minConNum = minConNumStr == null ? 1 : Integer.parseInt(minConNumStr);
				String maxConNumStr = jdbcProperties.getProperty(dbName + ".maxconnum");
				int maxConNum = maxConNumStr == null ? Runtime.getRuntime().availableProcessors() * 2 : Integer.parseInt(maxConNumStr);
				jdbcUtilMap.put(dbName, new JdbcUtil(dbName, url, username, password, initConNum, minConNum, maxConNum, druidProperties));
			} catch (Exception e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}


	public void add(String dbName, String url, String username, String password, int initconnum, int minconnum, int maxconnum, Properties druidProperties) {
		jdbcUtilMap.put(dbName, new JdbcUtil(dbName, url, username, password, initconnum, minconnum, maxconnum, druidProperties));
	}

	public JdbcUtil get(String dbName) {
		return jdbcUtilMap.get(dbName);
	}

	public void remove(String dbName) {
		jdbcUtilMap.get(dbName).closeDataSource();
		jdbcUtilMap.remove(dbName);
	}
}
