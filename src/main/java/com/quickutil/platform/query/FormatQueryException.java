package com.quickutil.platform.query;

/**
 * @author shijie.ruan
 */
public class FormatQueryException extends Exception {
	private String msg;

	FormatQueryException(String msg) { this.msg = msg; }
}
