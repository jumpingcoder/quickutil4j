package com.quickutil.platform.def;

import com.google.gson.GsonBuilder;
import com.quickutil.platform.JsonUtil;

public class Result {

	private boolean success = false;
	private Object message = null;
	private int code = 0;

	public Result(boolean success) {
		this.success = success;
	}

	public Result(boolean success, Object message) {
		this.success = success;
		this.message = message;
	}

	public Result(boolean success, Object message, int code) {
		this.success = success;
		this.message = message;
		this.code = code;
	}

	public boolean getSuccess() {
		return success;
	}

	public Object getMessage() {
		return message;
	}

	public int getCode() {
		return code;
	}

	public String toJson() {
		return JsonUtil.toJson(this);
	}

	public String toJsonWithFormat(GsonBuilder builder) {
		return JsonUtil.toJsonWithFormat(this, builder);
	}

	public Result clone() {
		return new Result(success, message, code);
	}
}
