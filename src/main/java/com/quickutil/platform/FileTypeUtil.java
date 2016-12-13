/**
 * 文件类型工具
 * 
 * @class FileTypeUtil
 * @author 0.5
 */
package com.quickutil.platform;

public class FileTypeUtil {

    public enum FileType {
        jpg("FFD8FF"), png("89504E47"), gif("47494638"), tiff("49492A00"), bmp("424D"), dwg("41433130"), psd("38425053"), rtf("7B5C7274"), xml("3C3F786D"), html(
                "68746D6C"), xls_doc("D0CF11E0"), pdf("25504446"), zip("504B0304"), rar("52617221"), wav(
                        "57415645"), avi("41564920"), ram("2E7261FD"), mp4("000000"), rm("2E524D46"), mpg("000001BA"), mov("6D6F6F76"), mid("4D546864"), mp3("49443303");
        private String value = "";

        private FileType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    /**
     * 获取文件类型
     * 
     * @param bt-字节数组
     * @return
     */
    public static FileType getFileType(byte[] bt) {
        byte[] headByte = new byte[28];
        for (int i = 0; i < 28 && i < bt.length; i++) {
            headByte[i] = bt[i];
        }
        String headString = CryptoUtil.byte2hex(headByte);
        if (headString.length() == 0)
            return null;
        headString = headString.toUpperCase();
        for (FileType type : FileType.values()) {
            if (headString.startsWith(type.getValue())) {
                return type;
            }
        }
        return null;
    }
}
