/**
 * IP工具
 * 
 * @class IpUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class IPUtil {

    /**
     * 获取本机ipv4地址列表
     * 
     * @return
     */
    public static List<String> getIpv4List() {
        List<String> ipList = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                Enumeration<InetAddress> addresses = allNetInterfaces.nextElement().getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address)
                        ipList.add(ip.getHostAddress());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ipList;
    }

    /**
     * 获取本机ipv6地址列表
     * 
     * @return
     */
    public static List<String> getIpv6List() {
        List<String> ipList = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                Enumeration<InetAddress> addresses = allNetInterfaces.nextElement().getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (!(ip instanceof Inet4Address))
                        ipList.add(ip.getHostAddress());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ipList;
    }
}
