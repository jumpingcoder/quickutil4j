package com.quickutil.platform.def;

public class AttachmentDef {

	public String fileName;
	public byte[] file;

	/**
	 * 附件定义
	 * 
	 * @param fileName-文件名
	 * @param file-文件内容
	 */
	public AttachmentDef(String fileName, byte[] file) {
		this.fileName = fileName;
		this.file = file;
	}
}
