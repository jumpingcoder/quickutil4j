package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;

import java.lang.management.ManagementFactory;
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
public class EnvironmentUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(EnvironmentUtil.class);
    private static String hostName = null;
    private static byte[] mac = null;

    static {
        try {
            InetAddress ia = InetAddress.getLocalHost();
            hostName = ia.getHostName();
            mac = NetworkInterface.getByInetAddress(ia).getHardwareAddress();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    /**
     * 获取本机host+md5(MAC地址)，因为性能较低，所以存到了静态变量
     */
    public static String getMachineFinger() {
        try {
            return hostName + ":" + CryptoUtil.md5Encode(mac);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取本机hostname
     */
    public static String getHostName() {
        return hostName;
    }

    /**
     * 获取本机mac
     */
    public static byte[] getMacAddress() {
        return mac;
    }

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
            LOGGER.error(Symbol.BLANK, e);
        }
        return ipList;
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
            LOGGER.error(Symbol.BLANK, e);
        }
        return ipList;
    }

    /**
     * 获取当前进程号
     */
    public static int getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();// "pid@hostname"
        return Integer.parseInt(name.substring(0, name.indexOf('@')));
    }

    /**
     * 获取当前线程号
     */
    public static long getThreadId() {
        return Thread.currentThread().getId();
    }

    public static void main(String[] args) {
        LOGGER.info(getMachineFinger());
    }

}
