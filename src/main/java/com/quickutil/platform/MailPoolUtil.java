package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 邮件发送工具-配置池
 *
 * @author 0.5
 */
public class MailPoolUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MailPoolUtil.class);

	private static Map<String, MailUtil> sessionMap = new HashMap<>();

	public MailPoolUtil() {
	}

	public MailPoolUtil(Properties mailProperties) {
		Enumeration<?> keys = mailProperties.propertyNames();
		List<String> keyList = new ArrayList<>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			if (!keyList.contains(key)) {
				keyList.add(key);
			}
		}
		for (String key : keyList) {
			try {
				String host = mailProperties.getProperty(key + ".host");
				String port = mailProperties.getProperty(key + ".port");
				String username = mailProperties.getProperty(key + ".username");
				String password = mailProperties.getProperty(key + ".password");
				if (host == null || port == null || username == null || password == null)
					throw new MissingParametersException("init requires host, port, username, password");
				String isSSLStr = mailProperties.getProperty(key + ".isSSL");
				boolean isSSL = isSSLStr == null ? false : Boolean.parseBoolean(isSSLStr);
				sessionMap.put(key, new MailUtil(host, port, username, password, isSSL));
			} catch (Exception e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}

	public void add(String key, String host, String port, String username, String password, boolean isSSL) {
		sessionMap.put(key, new MailUtil(host, port, username, password, isSSL));
	}

	public MailUtil get(String key) {
		return sessionMap.get(key);
	}

	public MailUtil remove(String key) {
		return sessionMap.remove(key);
	}
}
