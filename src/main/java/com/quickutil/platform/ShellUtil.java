/**
 * Shell调用工具
 * 
 * @class ShellUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.lang.management.ManagementFactory;

public class ShellUtil {

    /**
     * 获取当前进程号
     * 
     * @return
     */
    public static int getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();// "pid@hostname"
        return Integer.parseInt(name.substring(0, name.indexOf('@')));
    }

    /**
     * 获取当前线程号
     * 
     * @return
     */
    public static long getThreadId() {
        return Thread.currentThread().getId();
    }

    /**
     * 执行shell命令
     * 
     * @param command-shell命令
     * @return
     */
    public static String command(String command) {
        try {
            Process process = Runtime.getRuntime().exec(command);
            byte[] bt = FileUtil.stream2byte(process.getInputStream());
            String result = new String(bt);
            bt = FileUtil.stream2byte(process.getErrorStream());
            result = result + new String(bt);
            process.waitFor();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
