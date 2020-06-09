package com.quickutil.platform;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;

import javax.imageio.ImageIO;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

/**
 * 图像处理工具
 *
 * @author 0.5
 */
public class ImageUtil {
	
	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ImageUtil.class);

	public static String Format_JPG = "jpg";
	public static String Format_PNG = "png";

	/**
	 * 图片转ByteBuffer
	 * 
	 * @param bi-图片内容
	 * @param format-图片格式
	 * @return
	 */
	public static ByteBuffer imgToByte(BufferedImage bi, String format) {
		ByteBuffer buffer = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(bi, format, baos);
			byte[] bytes = baos.toByteArray();
			buffer = ByteBuffer.wrap(bytes);
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return buffer;
	}

	/**
	 * byte[]转图片
	 * 
	 * @param bt-字节数组
	 * @return
	 */
	public static BufferedImage byteToImage(byte[] bt) {
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(bt);
			return ImageIO.read(in);
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return null;
	}

	/**
	 * 读取文件为图片
	 * 
	 * @param filePath-文件路径
	 * @return
	 */
	public static BufferedImage readImage(String filePath) {
		try {
			return ImageIO.read(new File(filePath));
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return null;
	}

	/**
	 * 将图片写入文件
	 * 
	 * @param filePath-文件路径
	 * @param bi-图片
	 * @param format-图片格式
	 */
	public static boolean writeImage(String filePath, BufferedImage bi, String format) {
		try {
			File newfile = new File(filePath);
			if (!newfile.exists())
				newfile.createNewFile();
			ImageIO.write(bi, format, newfile);
			return true;
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return false;
	}

	/**
	 * 图像缩放
	 * 
	 * @param bi-图片
	 * @param width-宽度
	 * @param height-高度
	 * @return
	 */
	public static BufferedImage zoom(BufferedImage bi, int width, int height) {
		if (bi != null) {
			BufferedImage dstBi = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
			dstBi.getGraphics().drawImage(bi.getScaledInstance(width, height, Image.SCALE_SMOOTH), 0, 0, null);
			return dstBi;
		}
		return null;
	}

	/**
	 * 图像裁剪
	 * 
	 * @param bi-图片
	 * @param x-起点x轴坐标
	 * @param y-起点x轴坐标
	 * @param width-剪裁宽度
	 * @param height-剪裁高度
	 * @return
	 */
	public static BufferedImage cut(BufferedImage bi, int x, int y, int width, int height) {
		if (bi == null)
			return null;
		if (x < 0 || y < 0 || width < 1 || height < 1)
			return null;
		int srcWidth = bi.getWidth();
		int srcHeight = bi.getHeight();
		if (x + width > srcWidth || y + height > srcHeight)
			return null;
		BufferedImage newImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				newImage.setRGB(i, j, bi.getRGB(i + x, j + y));
			}
		}
		return newImage;
	}

}
