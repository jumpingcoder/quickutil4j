package com.quickutil.platform;

/**
 * @author shijie.ruan
 */
public class FormatQueryException extends Exception {
	public String msg;

	public FormatQueryException(String msg) { this.msg = msg; }
}
