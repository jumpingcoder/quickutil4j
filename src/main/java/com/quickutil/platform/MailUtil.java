package com.quickutil.platform;

import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.mail.Authenticator;
import javax.mail.Message.RecipientType;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

import com.quickutil.platform.CryptoUtil;

/**
 * 邮件模块
 * 
 * @class MailUtil
 * @author 0.5
 */

public class MailUtil {
	private static final String format = "text/html;charset=UTF-8";
	private static Properties mailProperties = null;
	private static Authenticator authenticator = null;

	public static void initMail(Properties properties) {
		mailProperties = properties;
		authenticator = new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				String userName = mailProperties.getProperty("mail.user");
				String password = (mailProperties.getProperty("mail.password"));
				return new PasswordAuthentication(userName, password);
			}
		};
	}

	public static boolean send(String[] toMails, String[] ccMails, String[] bccMails, String title, String text, List<byte[]> imageList) {
		try {
			InternetAddress[] toAddresses = new InternetAddress[toMails.length];
			for (int i = 0; i < toMails.length; i++) {
				toAddresses[i] = new InternetAddress(toMails[i]);
			}
			InternetAddress[] ccAddresses = new InternetAddress[ccMails.length];
			for (int i = 0; i < ccMails.length; i++) {
				ccAddresses[i] = new InternetAddress(ccMails[i]);
			}
			InternetAddress[] bccAddresses = new InternetAddress[bccMails.length];
			for (int i = 0; i < bccMails.length; i++) {
				bccAddresses[i] = new InternetAddress(bccMails[i]);
			}
			Session mailSession = Session.getInstance(mailProperties, authenticator);
			MimeMessage message = new MimeMessage(mailSession);
			message.setFrom(new InternetAddress(mailProperties.getProperty("mail.user")));
			message.setSubject(title);
			message.setContent(text, format);
			message.setRecipients(RecipientType.TO, toAddresses);
			message.setRecipients(RecipientType.CC, ccAddresses);
			message.setRecipients(RecipientType.BCC, bccAddresses);
			if (imageList != null) {
				// 构建图片资源
				MimeMultipart bodyMultipart = new MimeMultipart("related");
				MimeBodyPart htmlPart = new MimeBodyPart();
				StringBuilder content = new StringBuilder("<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"1280\"  style=\"width:1280px;\">");
				String random = CryptoUtil.randomMd5Code();
				for (int i = 0; i < imageList.size(); i++) {
					MimeBodyPart picPart = new MimeBodyPart();
					DataHandler picDataHandler = new DataHandler(new ByteArrayDataSource(imageList.get(i), "application/octet-stream"));
					picPart.setDataHandler(picDataHandler);
					picPart.setFileName(i + ".png");
					picPart.setHeader("Content-ID", random + i);
					bodyMultipart.addBodyPart(picPart);
					content.append("<tr><td><img src=\"cid:" + random + i + "\"/></td></tr>");
				}
				content.append("</table>");
				htmlPart.setContent(content.toString(), format);
				bodyMultipart.addBodyPart(htmlPart);
				message.setContent(bodyMultipart);
			}
			// 生成邮件
			message.saveChanges();
			Transport.send(message);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
