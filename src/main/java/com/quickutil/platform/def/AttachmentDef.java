package com.quickutil.platform.def;

public class AttachmentDef {

	public String fileName;
	public byte[] file;
	public boolean isImage;

	/**
	 * 附件定义
	 * 
	 * @param fileName-文件名
	 * @param file-文件内容
	 */
	public AttachmentDef(String fileName, byte[] file, boolean isImage) {
		this.fileName = fileName;
		this.file = file;
		this.isImage = isImage;
	}
}
