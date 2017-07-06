package com.quickutil.platform.def;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * @author shijie.ruan
 * 批量请求的返回类型
 * 一个 byte 值 isAllSuccess 表示 bulk 请求是否全部成功
 * bulkRequestError, 表示整个 bulk 请求失败的时候的错误信息
 * responseItems, bulk 中每一个请求的响应信息
 * isAllSuccess 为 0 时, 表示全部成功, bulkRequestError 和 responseItems 都为空
 * isAllSuccess 为 1 时表示 bulk 请求失败, 请获取 bulkRequestError
 * isAllSuccess 为 2 时表示部分请求失败, 请获取 responseItems
 */
public class BulkResponse {
	public static byte Success = 0;
	public static byte BulkRequestFail = 1;
	public static byte PortionFail = 2;

	private byte isSuccess;
	private JsonObject bulkRequestError = null;
	private JsonArray responseItems = null;

	public BulkResponse(byte isSuccess, JsonArray responseItems) {
		this.isSuccess = isSuccess;
		this.responseItems = responseItems;
	}

	public BulkResponse(byte isSuccess, JsonObject bulkRequestError) {
		this.bulkRequestError = bulkRequestError;
		this.isSuccess = isSuccess;
	}

	public BulkResponse(byte isSuccess) {
		this.isSuccess = isSuccess;
	}

	public boolean isSuccess() {
		return 0 == isSuccess;
	}

	public byte getIsSuccess() {
		return this.isSuccess;
	}

	public JsonArray getResponseItems() {
		return this.responseItems;
	}

	public JsonObject getBulkRequestError() {
		return this.bulkRequestError;
	}
}
