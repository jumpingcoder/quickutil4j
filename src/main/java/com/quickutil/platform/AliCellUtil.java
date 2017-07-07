/**
 * 阿里大鱼短信工具
 * 
 * @class AliCellUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.HttpResponse;

public class AliCellUtil {

	private static Map<String, Map<String, String>> dayuMap = new HashMap<String, Map<String, String>>();
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final String UTF8 = "utf-8";

	/**
	 * 初始化大鱼配置
	 * 
	 * @param properties-配置文件
	 * @return
	 */
	public static boolean init(Properties properties) {
		Enumeration<?> keys = properties.propertyNames();
		Set<String> keyList = new HashSet<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			keyList.add(key);
		}
		for (String key : keyList) {
			try {
				Map<String, String> map = new HashMap<String, String>();
				map.put("signname", properties.getProperty(key + ".signname"));
				map.put("appkey", properties.getProperty(key + ".appkey"));
				map.put("appsecret", properties.getProperty(key + ".appsecret"));
				map.put("template", properties.getProperty(key + ".template"));
				dayuMap.put(key, map);
			} catch (Exception e) {
				LogUtil.error(e, "阿里大鱼配置参数错误");
			}
		}
		return true;
	}

	private static final String signFormat = "app_key%sformatjsonmethodalibaba.aliqin.fc.sms.num.sendrec_num%ssign_methodhmacsms_free_sign_name%ssms_param%ssms_template_code%ssms_typenormaltimestamp%sv2.0";
	private static final String urlFormat = "http://gw.api.taobao.com/router/rest?app_key=%s&format=json&method=alibaba.aliqin.fc.sms.num.send&rec_num=%s&sign_method=hmac&sms_free_sign_name=%s&sms_param=%s&sms_template_code=%s&sms_type=normal&v=2.0&timestamp=%s&sign=%s";

	/**
	 * 
	 * 
	 * @param dayuName-大鱼名称
	 * @param cell-手机号
	 * @param message-发送的信息
	 * @return
	 */
	public static boolean sendMessage(String dayuName, String cell, String message) {
		try {
			String appkey = dayuMap.get(dayuName).get("appkey");
			String signname = dayuMap.get(dayuName).get("signname");
			String template = dayuMap.get(dayuName).get("template");
			String appsecret = dayuMap.get(dayuName).get("appsecret");
			Map<String, Object> sms_param_map = new HashMap<String, Object>();
			sms_param_map.put("content", message);
			String sms_param = JsonUtil.toJson(sms_param_map);
			String timestamp = sdf.format(new Date());
			String signContent = String.format(signFormat, appkey, cell, signname, sms_param, template, timestamp);
			String sign = CryptoUtil.HmacMD5Encrypt(signContent.getBytes(), appsecret).toUpperCase();
			String url = String.format(urlFormat, appkey, cell, URLEncoder.encode(signname, UTF8), URLEncoder.encode(sms_param, UTF8), template, URLEncoder.encode(timestamp, UTF8),
					sign);
			HttpResponse response = HttpUtil.httpPost(url);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			System.out.println(result);
			return JsonUtil.toJsonMap(result).getAsJsonObject("alibaba_aliqin_fc_sms_num_send_response").getAsJsonObject("result").get("success").getAsBoolean();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 
	 * @param dayuName-大鱼名称
	 * @param cell-手机号
	 * @param message-发送的信息模板
	 * @return
	 */
	public static boolean sendMessage(String dayuName, String cell, Map<String, Object> smsParamMap) {
		try {
			String appkey = dayuMap.get(dayuName).get("appkey");
			String signname = dayuMap.get(dayuName).get("signname");
			String template = dayuMap.get(dayuName).get("template");
			String appsecret = dayuMap.get(dayuName).get("appsecret");
			String sms_param = JsonUtil.toJson(smsParamMap);
			String timestamp = sdf.format(new Date());
			String signContent = String.format(signFormat, appkey, cell, signname, sms_param, template, timestamp);
			String sign = CryptoUtil.HmacMD5Encrypt(signContent.getBytes(), appsecret).toUpperCase();
			String url = String.format(urlFormat, appkey, cell, URLEncoder.encode(signname, UTF8), URLEncoder.encode(sms_param, UTF8), template, URLEncoder.encode(timestamp, UTF8),
					sign);
			HttpResponse response = HttpUtil.httpPost(url);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			System.out.println(result);
			return JsonUtil.toJsonMap(result).getAsJsonObject("alibaba_aliqin_fc_sms_num_send_response").getAsJsonObject("result").get("success").getAsBoolean();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
