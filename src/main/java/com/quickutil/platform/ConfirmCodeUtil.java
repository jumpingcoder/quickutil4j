/**
 * 验证码生成工具
 * 
 * @class ConfirmCodeUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.Random;

import com.quickutil.platform.ImageUtil;

public class ConfirmCodeUtil {
	private static char[] codeSequence = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2', '3', '4', '5', '6',
			'7', '8', '9' };
	private static char[] numSequence = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	private static Random random = new Random();
	private static final String fontType = "Arial";

	/**
	 * 随机生成混合验证码
	 * 
	 * @param codeLength-验证码长度
	 * @return
	 */
	public static String getRandomCode(int codeLength) {
		StringBuffer confirmCode = new StringBuffer();
		for (int i = 0; i < codeLength; i++) {
			confirmCode.append(codeSequence[random.nextInt(codeSequence.length)]);
		}
		return confirmCode.toString();
	}

	/**
	 * 随机生成纯数字验证码
	 * 
	 * @param codeLength-验证码长度
	 * @return
	 */
	public static String getRandomNumCode(int codeLength) {
		StringBuffer confirmCode = new StringBuffer();
		for (int i = 0; i < codeLength; i++) {
			confirmCode.append(numSequence[random.nextInt(numSequence.length)]);
		}
		return confirmCode.toString();
	}

	/**
	 * 生成验证码图片
	 * 
	 * @param confirmcode-验证码
	 * @param width-图片宽度
	 * @param height-图片高度
	 * @return
	 */
	public static byte[] getPicture(String confirmcode, int width, int height) {
		int lineCount = height / 2;
		int fontSize = height * 3 / 4;
		int codeWidth = confirmcode.length() + 2;
		BufferedImage buffImg = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		// 图像buffer
		Graphics2D graph = buffImg.createGraphics();
		graph.setColor(Color.WHITE);
		graph.fillRect(0, 0, width, height);
		Font font = new Font(fontType, Font.PLAIN, fontSize);
		graph.setFont(font);
		// 绘制字符并旋转
		for (int i = 0; i < confirmcode.length(); i++) {
			graph.setColor(new Color(random.nextInt(255), random.nextInt(255), random.nextInt(255)));
			graph.drawString(confirmcode.charAt(i) + "", (i + 1) * width / codeWidth, fontSize);
			if (i == 0)
				graph.rotate(0.3, (i + 1) * width / codeWidth, height / 2);
			else if (i % 2 == 0)
				graph.rotate(0.6, (i + 1) * width / codeWidth, fontSize / 2);
			else
				graph.rotate(-0.6, (i + 1) * width / codeWidth, fontSize / 2);
		}
		// 创建干扰线
		for (int i = 0; i < lineCount; i++) {
			int xs = random.nextInt(width * 2 / 3);
			int ys = random.nextInt(height * 2 / 3);
			int xe = xs + random.nextInt(width / 3);
			int ye = ys + random.nextInt(height / 3);
			graph.setColor(new Color(random.nextInt(255), random.nextInt(255), random.nextInt(255)));
			graph.drawLine(xs, ys, xe, ye);
		}
		return ImageUtil.imgToByte(buffImg, ImageUtil.Format_JPG).array();
	}
}
