package com.quickutil.platform;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Data.Ga;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GAUtil {

	private static final Logger logger = LoggerFactory.getLogger(GAUtil.class);
	private static Map<String, Ga> gaClientMap = new HashMap<String, Ga>();

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
				addGaClient(key, clientID, PropertiesUtil.getInputStream(p12Path), applicationName);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	public static void addGaClient(String key, String clientID, InputStream p12InputStream, String applicationName) {
		try {
			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			KeyStore keystore = KeyStore.getInstance("PKCS12");
			keystore.load(p12InputStream, null);
			PrivateKey privateKey = (PrivateKey) keystore.getKey("privatekey", "notasecret".toCharArray());
			GoogleCredential credential = new GoogleCredential.Builder().setTransport(httpTransport).setJsonFactory(jsonFactory).setServiceAccountId(clientID).setServiceAccountPrivateKey(privateKey)
					.setServiceAccountScopes(Collections.singleton(AnalyticsScopes.ANALYTICS_READONLY)).build();
			Ga ga = new Analytics.Builder(httpTransport, jsonFactory, credential).setApplicationName(applicationName).build().data().ga();
			gaClientMap.put(key, ga);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	public static Ga getGaClient(String key) {
		return gaClientMap.get(key);
	}

	public static void removeGaClient(String key) {
		gaClientMap.remove(key);
	}
}
