package com.quickutil.platform.exception;

/**
 * @author 0.5
 */
public class MissingParametersException extends Exception {

	private static final long serialVersionUID = 7626968747369866562L;
	public String msg;

	public MissingParametersException(String msg) {
		this.msg = msg;
	}
}
