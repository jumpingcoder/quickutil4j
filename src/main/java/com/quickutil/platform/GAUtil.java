package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Data;
import com.google.api.services.analytics.Analytics.Data.Ga;
import com.google.api.services.analytics.Analytics.Data.Realtime;
import com.google.api.services.analytics.AnalyticsScopes;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.LoggerFactory;

/**
 * GA工具
 * 官方文档详见https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/installed-java
 *
 * @author 0.5
 */
public class GAUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(GAUtil.class);
	private static Map<String, Ga> gaClientMap = new HashMap<String, Ga>();
	private static Map<String, String> gaIdsMap = new HashMap<>();
	private static Map<String, Realtime> gaRTClientMap = new HashMap<>();

	public static void addGaClient(Properties properties) {
		Enumeration<?> keys = properties.propertyNames();
		List<String> keyList = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			if (!keyList.contains(key)) {
				keyList.add(key);
			}
		}
		for (String key : keyList) {
			try {
				String clientID = properties.getProperty(key + ".clientID");
				String p12Path = properties.getProperty(key + ".p12Path");
				String applicationName = properties.getProperty(key + ".applicationName");
				String ids = properties.getProperty(key + ".ids");
				gaIdsMap.put(key, ids);
				addGaClient(key, clientID, PropertiesUtil.getInputStream(p12Path), applicationName);
				addGaRTClient(key, clientID, PropertiesUtil.getInputStream(p12Path), applicationName);
			} catch (Exception e) {
				LOGGER.error("", e);
			}
		}
	}

	public static void addGaClient(String key, String clientID, InputStream p12InputStream, String applicationName) {
		try {
			Data data = buildData(clientID, p12InputStream, applicationName);
			if(data != null){
				Ga ga = data.ga();
				gaClientMap.put(key, ga);
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	private static Data buildData(String clientID, InputStream p12InputStream, String applicationName){
		try {
			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			KeyStore keystore = KeyStore.getInstance("PKCS12");
			keystore.load(p12InputStream, null);
			PrivateKey privateKey = (PrivateKey) keystore.getKey("privatekey", "notasecret".toCharArray());
			GoogleCredential credential = new GoogleCredential.Builder().setTransport(httpTransport).setJsonFactory(jsonFactory).setServiceAccountId(clientID).setServiceAccountPrivateKey(privateKey)
					.setServiceAccountScopes(Collections.singleton(AnalyticsScopes.ANALYTICS_READONLY)).build();
			return new Analytics.Builder(httpTransport, jsonFactory, credential).setApplicationName(applicationName).build().data();
		} catch (Exception e) {
			LOGGER.error("", e);
			return null;
		}
	}

	public static void addGaRTClient(String key, String clientID, InputStream p12InputStream, String applicationName) {
		try {
			Data data = buildData(clientID, p12InputStream, applicationName);
			if(data != null){
				Realtime rt = data.realtime();
				gaRTClientMap.put(key, rt);
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	public static Ga getGaClient(String key) {
		return gaClientMap.get(key);
	}

	public static Realtime getGaRTClient(String key){
		return gaRTClientMap.get(key);
	}

	public static void removeGaClient(String key) {
		gaClientMap.remove(key);
	}

	public static void removeGaRTClient(String key) {
		gaRTClientMap.remove(key);
	}

	public static String getGaIds(String key) {
		return gaIdsMap.get(key);
	}
}
