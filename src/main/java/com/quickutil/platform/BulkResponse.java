package com.quickutil.platform;

/**
 * @author shijie.ruan
 * 批量请求的返回类型
 * 包含一个 boolean 值表示是否全部成功
 * 和一个 byte 数组表示每一条请求的错误码, 0 表示成功, 1 表示失败,后续可再增加错误码
 */
public class BulkResponse {
	public static byte itemFalse = 1;

	private boolean isSuccess;
	private byte[] responseItem;

	public BulkResponse(boolean isSuccess, byte[] responseItem) {
		this.isSuccess = isSuccess;
		this.responseItem = responseItem;
	}

	public boolean hasFail() {
		return !isSuccess;
	}

	public byte[] getResponseItem() {
		return responseItem;
	}
}
