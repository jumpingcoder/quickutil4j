package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * IP工具
 *
 * @author 0.5
 */
public class IPUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(IPUtil.class);

	/**
	 * 获取本机ipv4地址列表
	 */
	public static List<String> getIpv4List() {
		List<String> ipList = new ArrayList<String>();
		try {
			Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
			while (allNetInterfaces.hasMoreElements()) {
				Enumeration<InetAddress> addresses = allNetInterfaces.nextElement().getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress ip = addresses.nextElement();
					if (ip instanceof Inet4Address) {
						ipList.add(ip.getHostAddress());
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return ipList;
	}

	/**
	 * 获取本机ipv4地址列表序列化
	 */

	public static String getIpv4ListString() {
		StringBuilder sb = new StringBuilder();
		List<String> ipv4List = getIpv4List();
		for (String ipv4 : ipv4List) {
			sb.append(ipv4 + ",");
		}
		if (sb.length() > 0) {
			sb = sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	/**
	 * 获取本机ipv6地址列表
	 */
	public static List<String> getIpv6List() {
		List<String> ipList = new ArrayList<String>();
		try {
			Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
			while (allNetInterfaces.hasMoreElements()) {
				Enumeration<InetAddress> addresses = allNetInterfaces.nextElement().getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress ip = addresses.nextElement();
					if (!(ip instanceof Inet4Address)) {
						ipList.add(ip.getHostAddress());
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return ipList;
	}

	public static String getIpv6ListString() {
		StringBuilder sb = new StringBuilder();
		List<String> ipv6List = getIpv6List();
		for (String ipv6 : ipv6List) {
			sb.append(ipv6 + ",");
		}
		if (sb.length() > 0) {
			sb = sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}
}
