package com.quickutil.platform.def;

public class AttachmentDef {

	public String fileName;
	public byte[] file;
	public boolean isImage;

	public AttachmentDef(String fileName, byte[] file, boolean isImage) {
		this.fileName = fileName;
		this.file = file;
		this.isImage = isImage;
	}
}
