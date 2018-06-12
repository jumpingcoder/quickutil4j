/**
 * 配置文件工具
 * 
 * @class PropertiesUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

public class PropertiesUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(PropertiesUtil.class);

	/**
	 * 获取资源文件路径-jar包不适用
	 * 
	 * @param filePath-资源文件路径
	 * @return
	 */
	public static String getResourcePath(String filePath) {
		return new PropertiesUtil().getResourcePathIn(filePath);
	}

	private String getResourcePathIn(String filePath) {
		try {
			URL url = this.getClass().getClassLoader().getResource("");
			if (url != null)
				return url.getPath() + filePath;
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	/**
	 * 获取资源文件字节流
	 * 
	 * @param filePath-资源文件路径
	 * @return
	 */
	public static InputStream getInputStream(String filePath) {
		String resourcePath = getResourcePath(filePath);
		if (resourcePath != null)
			return FileUtil.file2Stream(resourcePath);
		return new PropertiesUtil().getResourceStreamIn(filePath);
	}

	private InputStream getResourceStreamIn(String filePath) {
		return this.getClass().getResourceAsStream("/resources/" + filePath);
	}

	/**
	 * 将资源文件读取为Properties
	 * 
	 * @param filePath-资源文件路径
	 * @return
	 */
	public static Properties getProperties(String filePath) {
		return getProperties(getInputStream(filePath));
	}

	/**
	 * 将字节流读取为properties
	 * 
	 * @param stream-文件字节流
	 * @return
	 */
	public static Properties getProperties(InputStream stream) {
		Properties properties = new Properties();
		try {
			properties.load(stream);
			return properties;
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	/**
	 * 获取非标准aes解密properties
	 * 
	 * @param filePath-资源文件路径
	 * @param password-解密密钥
	 * @return
	 */
	public static Properties getPropertiesWithKey(String filePath, String password) {
		Properties properties = getProperties(filePath);
		for (Object pkey : properties.keySet()) {
			String pvalue = properties.getProperty(pkey.toString());
			String result = CryptoUtil.aesDecryptStr(pvalue, password);
			properties.setProperty(pkey.toString(), result == null ? pvalue : result);
		}
		return properties;
	}

	/**
	 * 获取非标准aes解密properties
	 * 
	 * @param stream-文件字节流
	 * @param password-解密密钥
	 * @return
	 */
	public static Properties getPropertiesWithKey(InputStream stream, String password) {
		Properties properties = getProperties(stream);
		for (Object pkey : properties.keySet()) {
			String pvalue = properties.getProperty(pkey.toString());
			properties.setProperty(pkey.toString(), CryptoUtil.aesDecryptStr(pvalue, password));
		}
		return properties;
	}

}
