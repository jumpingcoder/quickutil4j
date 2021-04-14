package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;

/**
 * Shell调用工具
 *
 * @author 0.5
 */
public class ShellUtil {
	
	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ShellUtil.class);

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
			LOGGER.error(Symbol.BLANK,e);
			return null;
		}
	}

	/**
	 * 打印进度条
	 * 
	 * @param percentage-进度
	 */
	public static synchronized String printProgress(double percentage) {
		StringBuilder sb = new StringBuilder();
		final int len = 50;
		int i = 0;
		sb.append("[");
		for (; i <= (int) (percentage * len); i++) {
			sb.append("=");
		}
		for (; i <= len; i++) {
			sb.append(" ");
		}
		sb.append("]");
		return sb.toString();
	}
}
