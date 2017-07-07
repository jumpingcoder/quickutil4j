package com.quickutil.platform.exception;

/**
 * @author shijie.ruan
 */
public class FormatQueryException extends Exception {

	private static final long serialVersionUID = -6194921559482495777L;
	public String msg;

	public FormatQueryException(String msg) {
		this.msg = msg;
	}
}
