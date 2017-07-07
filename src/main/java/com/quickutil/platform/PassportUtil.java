/**
 * 登录工具-暂不开放
 * 
 * @class PassportUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpResponse;

public class PassportUtil {

	private static String PassportHost = null;
	private static String PassportAppid = null;
	private static String PassportAppkey = null;
	//
	private static final String showLoginSec = "appid=%s&requestUrl=%s&redirectUrl=%s&time=%s";
	private static final String showLoginReq = "/passport?appid=%s&requestUrl=%s&redirectUrl=%s&time=%s&secretkey=%s";
	private static final String loginFromTokenSec = "appid=%s&username=%s&token=%s&time=%s";
	private static final String loginFromTokenReq = "/passport/loginFromToken?appid=%s&username=%s&token=%s&time=%s&secretkey=%s";
	//
	private static final String payFromStatementidSec = "appid=%s&username=%s&statementid=%s&time=%s&appinfo1=%s&appinfo2=%s&appinfo3=%s&appinfo4=%s&appinfo5=%s&appremark=%s";
	private static final String payFromStatementidReq = "/pay/payFromStatementid?appid=%s&username=%s&statementid=%s&time=%s&appinfo1=%s&appinfo2=%s&appinfo3=%s&appinfo4=%s&appinfo5=%s&appremark=%s&secretkey=%s";
	//
	private static final String cellMessageSec = "appid=%s&cell=%s&time=%s";
	private static final String cellMessageReq = "/cell/send?appid=%s&cell=%s&time=%s&secretkey=%s";
	//
	private static final String mailMessageSec = "appid=%s&cell=%s&time=%s";
	private static final String mailMessageReq = "/mail/send?appid=%s&mail=%s&time=%s&secretkey=%s";
	//
	private static final String pushMessageSec = "appid=%s&message=%s&time=%s";
	private static final String pushMessageReq = "/push/send?appid=%s&device=%s&time=%s&secretkey=%s";

	/**
	 * 初始化配置
	 * 
	 * @param properties-passport配置
	 * @return
	 */
	public static boolean init(Properties properties) {
		Enumeration<?> keys = properties.propertyNames();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			if (key.equals("PassportHost"))
				PassportHost = properties.getProperty(key);
			else if (key.equals("PassportAppid"))
				PassportAppid = properties.getProperty(key);
			else if (key.equals("PassportAppkey"))
				PassportAppkey = properties.getProperty(key);
		}
		if (PassportHost == null || PassportAppid == null || PassportAppkey == null) {
			System.out.println("passport.properties 参数不正确");
			return false;
		}
		return true;
	}

	/**
	 * 获取登录服务的host
	 * 
	 * @return
	 */
	public static String getPassPortHost() {
		return PassportHost;
	}

	/**
	 * 网页打开passport登录页
	 * 
	 * @param requestUrl-登录成功后验证Token的url
	 * @param redirectUrl-验证成功后跳转的url
	 * @return
	 */
	public static String showLogin(String requestUrl, String redirectUrl) {
		try {
			requestUrl = URLEncoder.encode(requestUrl, "UTF-8");
			redirectUrl = URLEncoder.encode(redirectUrl, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		long time = System.currentTimeMillis();
		String secretkey = CryptoUtil.HmacSHA1Encrypt(String.format(showLoginSec, PassportAppid, requestUrl, redirectUrl, time).getBytes(), PassportAppkey);
		return String.format(PassportHost + showLoginReq, PassportAppid, requestUrl, redirectUrl, time, secretkey);
	}

	/**
	 * 服务端验证验证Token
	 * 
	 * @param username-用户名
	 * @param token-身份令牌
	 * @return
	 */
	public static boolean loginFromToken(String username, String token) {
		try {
			if (username == null || token == null)
				return false;
			long time = System.currentTimeMillis();
			String secretkey = CryptoUtil.HmacSHA1Encrypt(String.format(loginFromTokenSec, PassportAppid, username, token, time).getBytes(), PassportAppkey);
			HttpResponse response = HttpUtil.httpPost(String.format(PassportHost + loginFromTokenReq, PassportAppid, username, token, time, secretkey), null, null, null, null);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			Map<String, Object> map = JsonUtil.toMap(result);
			if ((Boolean) map.get("success"))
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 支付验证
	 * 
	 * @param username-用户名
	 * @param statementid-流水号
	 * @param appinfo1-交易信息字段
	 * @param appinfo2-交易信息字段
	 * @param appinfo3-交易信息字段
	 * @param appinfo4-交易信息字段
	 * @param appinfo5-交易信息字段
	 * @param appremark--交易信息备注
	 * @return
	 */
	public static Map<String, Object> payFromStatementid(String username, String statementid, String appinfo1, String appinfo2, String appinfo3, String appinfo4, String appinfo5,
			String appremark) {
		try {
			if (username == null || statementid == null)
				return null;
			long time = System.currentTimeMillis();
			String secretkey = CryptoUtil.HmacSHA1Encrypt(
					String.format(payFromStatementidSec, PassportAppid, username, statementid, time, appinfo1, appinfo2, appinfo3, appinfo4, appinfo5, appremark).getBytes(),
					PassportAppkey);
			HttpResponse response = HttpUtil.httpPost(String.format(PassportHost + payFromStatementidReq, PassportAppid, username, statementid, time, appinfo1, appinfo2, appinfo3,
					appinfo4, appinfo5, appremark, secretkey), null, null, null, null);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			return JsonUtil.toMap(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 短信发送服务
	 * 
	 * @param cellList-手机号数组
	 * @param content-短信的内容
	 * @return
	 */
	public static boolean cellMessage(String[] cellList, String content) {
		try {
			if (cellList == null)
				return false;
			StringBuffer cellSb = new StringBuffer();
			for (String cell : cellList) {
				cellSb.append(cell);
				cellSb.append(",");
			}
			String cellContent = cellSb.toString();
			cellContent = cellContent.substring(0, cellContent.length() - 1);
			long time = System.currentTimeMillis();
			String secretkey = CryptoUtil.HmacSHA1Encrypt(String.format(cellMessageSec, PassportAppid, cellContent, time).getBytes(), PassportAppkey);
			HttpResponse response = HttpUtil.httpPost(String.format(PassportHost + cellMessageReq, PassportAppid, cellContent, time, secretkey), null, null, null, null);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			Map<String, Object> map = JsonUtil.toMap(result);
			if ((Boolean) map.get("success"))
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 邮件发送
	 * 
	 * @param mailList-邮箱地址数组
	 * @param content-邮件的内容
	 * @return
	 */
	public static boolean mailMessage(String[] mailList, String content) {
		try {
			if (mailList == null)
				return false;
			StringBuffer mailSb = new StringBuffer();
			for (String mail : mailList) {
				mailSb.append(mail);
				mailSb.append(",");
			}
			String mailContent = mailSb.toString();
			mailContent = mailContent.substring(0, mailContent.length() - 1);
			long time = System.currentTimeMillis();
			String secretkey = CryptoUtil.HmacSHA1Encrypt(String.format(mailMessageSec, PassportAppid, mailContent, time).getBytes(), PassportAppkey);
			HttpResponse response = HttpUtil.httpPost(String.format(PassportHost + mailMessageReq, PassportAppid, mailContent, time, secretkey), null, null, null, null);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			Map<String, Object> map = JsonUtil.toMap(result);
			if ((Boolean) map.get("success"))
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 消息推送
	 * 
	 * @param deviceList-设备编号数组
	 * @param content-推送的内容
	 * @return
	 */
	public static boolean pushMessage(String[] deviceList, String content) {
		try {
			if (deviceList == null)
				return false;
			StringBuffer deviceSb = new StringBuffer();
			for (String device : deviceList) {
				deviceSb.append(device);
				deviceSb.append(",");
			}
			String deviceContent = deviceSb.toString();
			deviceContent = deviceContent.substring(0, deviceContent.length() - 1);
			long time = System.currentTimeMillis();
			String secretkey = CryptoUtil.HmacSHA1Encrypt(String.format(pushMessageSec, PassportAppid, deviceContent, time).getBytes(), PassportAppkey);
			HttpResponse response = HttpUtil.httpPost(String.format(PassportHost + pushMessageReq, PassportAppid, deviceContent, time, secretkey), null, null, null, null);
			String result = new String(FileUtil.stream2byte(response.getEntity().getContent()));
			Map<String, Object> map = JsonUtil.toMap(result);
			if ((Boolean) map.get("success"))
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
