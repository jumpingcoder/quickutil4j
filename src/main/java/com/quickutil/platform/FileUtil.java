package com.quickutil.platform;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

/**
 * 文件工具
 *
 * @author 0.5
 */
public class FileUtil {
	
	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(FileUtil.class);

	/**
	 * 获取程序运行时路径
	 * 
	 * @return 路径
	 */
	public static String getCurrentPath() {
		File file = new File("");
		return file.getAbsolutePath();
	}

	/**
	 * 创建文件夹
	 * 
	 * @param dirPath-文件夹路径
	 * @return 是否成功
	 */
	public static boolean mkdirByFile(String dirPath) {
		try {
			File file = new File(dirPath);
			if (!file.exists())
				return file.mkdirs();
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 读取文件到byte[]
	 * 
	 * @param filePath-文件路径
	 * @return 文件字节数组
	 */
	public static byte[] file2Byte(String filePath) {
		File file = new File(filePath);
		FileChannel channel = null;
		FileInputStream stream = null;
		ByteBuffer byteBuffer = null;
		try {
			if (!file.exists())
				return null;
			stream = new FileInputStream(file);
			channel = stream.getChannel();
			byteBuffer = ByteBuffer.allocate((int) channel.size());
			while ((channel.read(byteBuffer)) > 0) {
			}
			channel.close();
			stream.close();
			return byteBuffer.array();
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return null;
	}

	/**
	 * 将byte[]写入文件
	 * 
	 * @param filePath-文件路径
	 * @param bt-字节数组
	 * @return 是否成功
	 */
	public static boolean byte2File(String filePath, byte[] bt) {
		try {
			File file = new File(filePath);
			if (!file.getParentFile().exists())
				file.getParentFile().mkdirs();
			FileOutputStream out = new FileOutputStream(filePath, false);// true追加
			out.write(bt);
			out.flush();
			out.close();
			return true;
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 读取文件到inputstream
	 * 
	 * @param filePath-文件路径
	 * @return 字节流
	 */
	public static InputStream file2Stream(String filePath) {
		try {
			return new FileInputStream(new File(filePath));
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return null;
	}

	/**
	 * inputstream转byte[]
	 * 
	 * @param stream-输入字节流
	 * @return 字节数组
	 */
	public static byte[] stream2byte(InputStream stream) {
		if (stream == null)
			return null;
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			byte[] buffer = new byte[4096];
			int n = 0;
			while (-1 != (n = stream.read(buffer))) {
				output.write(buffer, 0, n);
			}
		} catch (Exception e) {
			LOGGER.error("",e);
		} finally {
			try {
				output.close();
				stream.close();
			} catch (IOException e) {
				LOGGER.error("",e);
			}
		}
		return output.toByteArray();
	}

	/**
	 * inputstream转string
	 * 
	 * @param stream-输入字节流
	 * @return 字符串
	 */
	public static String stream2string(InputStream stream) {
		return new String(stream2byte(stream));
	}

	/**
	 * 读取文件到string
	 * 
	 * @param filePath-文件路径
	 * @return 文本
	 */
	public static String file2String(String filePath) {
		try {
			return new String(file2Byte(filePath));
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return null;
	}

	/**
	 * 将string写入文件
	 * 
	 * @param filePath-文件路径
	 * @param content-字符串内容
	 * @param append-是否追加
	 * @return 是否成功
	 */
	public static boolean string2File(String filePath, String content, boolean append) {
		File file = new File(filePath);
		File parent = file.getParentFile();
		mkdirByFile(parent.getAbsolutePath());
		OutputStreamWriter out = null;
		try {
			out = new OutputStreamWriter(new FileOutputStream(file, append), "UTF-8");
			out.write(content);
			out.flush();
			out.close();
			return true;
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 读取文件到List<String>
	 * 
	 * @param filePath-文件路径
	 * @return 文本列表
	 */
	public static List<String> readFileByLine(String filePath) {
		List<String> list = new ArrayList<String>();
		try {
			BufferedReader bw = new BufferedReader(new FileReader(new File(filePath)));
			String line = null;
			while ((line = bw.readLine()) != null) {
				list.add(line);
			}
			bw.close();
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return list;
	}

	/**
	 * List<String>写入文件
	 * 
	 * @param filename-文件路径
	 * @param contentList-字符串数组
	 * @param append-是否追加
	 * @return 是否成功
	 */
	public static boolean writeFileByLine(String filename, List<String> contentList, boolean append) {
		mkdirByFile(filename);
		File file = new File(filename);
		try {
			FileWriter writer = new FileWriter(file, append);
			for (String content : contentList)
				writer.write(content + "\r\n");
			writer.close();
			return true;
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 读输入流并写文件到filePath
	 * @param input - 输入流
	 * @param filePath - 写入文件路径
	 * @param append - 是否追加写
	 */
	public static void writeFile(InputStream input, String filePath, boolean append) {
		try(FileOutputStream fileOutputStream = new FileOutputStream(filePath, append)) {
			int byteRead = -1;
			byte[] buffer = new byte[1024 * 2];
			while ((byteRead = input.read(buffer)) != -1){
				fileOutputStream.write(buffer, 0, byteRead);
			}
		} catch (Exception e){
			LOGGER.error("", e);
		}
	}

	/**
	 * 获取某目录下文件路径（不含子文件夹内文件）
	 * 
	 * @param dirPath-文件夹路径
	 * @param withDirectory-是否包含文件夹名
	 * @return 文件路径列表
	 */
	public static List<String> getDirFiles(String dirPath, boolean withDirectory) {
		List<String> filepaths = new ArrayList<String>();
		try {
			File dir = new File(dirPath);
			File file[] = dir.listFiles();
			for (int i = 0; i < file.length; i++) {
				if (file[i].isDirectory()) {
					if (withDirectory)
						filepaths.add(file[i].getAbsolutePath());
				} else
					filepaths.add(file[i].getAbsolutePath());
			}
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return filepaths;
	}

	/**
	 * 获取某目录下全部文件路径（含子文件夹内文件）
	 * 
	 * @param dirPath-文件夹路径
	 * @param filter-关键词，过滤不需要的文件夹或文件
	 * @return 文件路径列表
	 */
	public static List<String> getAllFilePath(String dirPath, List<String> filter) {
		List<String> filePaths = new ArrayList<String>();
		File dir = new File(dirPath);
		if (!dir.exists())
			return filePaths;
		for (File file : dir.listFiles()) {
			if (filter != null && filter.contains(file.getName()))
				continue;
			if (file.isDirectory())
				filePaths.addAll(getAllFilePath(file.getAbsolutePath(), filter));
			else
				filePaths.add(file.getAbsolutePath());
		}
		return filePaths;
	}

	/**
	 * 拷贝文件
	 * 
	 * @param fromFile-源文件路径
	 * @param toFile-目标文件路径
	 * @return 是否成功
	 */
	public static boolean copyFile(String fromFile, String toFile) {
		try {
			byte[] bt = file2Byte(fromFile);
			return byte2File(toFile, bt);
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 移动文件
	 * 
	 * @param fromFile-源文件路径
	 * @param toFile-目标文件路径
	 * @return 是否成功
	 */
	public static boolean moveFile(String fromFile, String toFile) {
		try {
			File oldFile = new File(fromFile);
			File newFile = new File(toFile);
			if (!newFile.getParentFile().exists())
				newFile.getParentFile().mkdirs();
			oldFile.renameTo(newFile);
			return true;
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 删除文件或目录，如果是目录则删除整个目录下的所有文件
	 * 
	 * @param filePath-文件路径
	 * @return 是否成功
	 */
	public static boolean deleteFile(String filePath) {
		try {
			File file = new File(filePath);
			if(file.exists()){
			    if(file.isDirectory()){
                    for(String path : file.list()){
                        deleteFile(file.getPath() + File.separator + path);
                    }
                }
                return file.delete();
            }
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 多个byte[]合并为一个byte[]
	 * 
	 * @param list-多个字节数组
	 * @return 字节数组
	 */
	public static byte[] joinbyte(List<byte[]> list) {
		int totallength = 0;
		for (int i = 0; i < list.size(); i++) {
			totallength += list.get(i).length;
		}
		byte[] bt = new byte[totallength];
		int index = 0;
		for (int i = 0; i < list.size(); i++) {
			System.arraycopy(list.get(i), 0, bt, index, list.get(i).length);
			index = index + list.get(i).length;
		}
		return bt;
	}

	/**
	 * 一个byte[]切割为多个byte[]
	 * 
	 * @param bt-字节数组
	 * @param chunkLength-切割块的大小
	 * @return 字节数组列表
	 */
	public static List<byte[]> cutbyte(byte[] bt, int chunkLength) {
		List<byte[]> list = new ArrayList<byte[]>();
		int btlength = bt.length;
		for (int i = 0; i <= btlength / chunkLength; i++) {
			byte[] chunkbt = new byte[Math.min(btlength - i * chunkLength, chunkLength)];
			System.arraycopy(bt, i * chunkLength, chunkbt, 0, Math.min(btlength - i * chunkLength, chunkLength));
			list.add(chunkbt);
		}
		return list;
	}

	/**
	 * 获取文件后缀
	 * 
	 * @param filePath-文件路径
	 * @return 文件后缀
	 */
	public static String getSuffix(String filePath) {
		int index = filePath.lastIndexOf(".");
		if (index == -1)
			return "";
		else
			return filePath.substring(index);
	}

	public enum FileType {
		jpg("FFD8FF"), png("89504E47"), gif("47494638"), tiff("49492A00"), bmp("424D"), dwg("41433130"), psd("38425053"), rtf("7B5C7274"), xml("3C3F786D"), html("68746D6C"), xls_doc("D0CF11E0"), pdf(
				"25504446"), zip("504B0304"), rar(
						"52617221"), wav("57415645"), avi("41564920"), ram("2E7261FD"), mp4("000000"), rm("2E524D46"), mpg("000001BA"), mov("6D6F6F76"), mid("4D546864"), mp3("49443303");
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
	 * @return 文件类型
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
