/**
 * 邮件发送工具
 * 
 * @class MailUtil
 * @author 0.5
 */
package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.def.AttachmentDef;
import org.slf4j.LoggerFactory;

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
import java.util.*;

public class MailPoolUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MailPoolUtil.class);

	private static Map<String, Session> sessionMap = new HashMap<String, Session>();

	/**
	 * 初始化
	 * 
	 * @param properties-配置
	 */
	public static void addMailSession(Properties properties) {
		Map<String, Properties> map = new HashMap<String, Properties>();
		Enumeration<?> keys = properties.propertyNames();
		List<String> sessionList = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String sessionName = key.split("\\.")[0];
			if (!sessionList.contains(sessionName)) {
				map.put(sessionName, new Properties());
				sessionList.add(sessionName);
			}
			map.get(sessionName).setProperty("mail" + key.substring(key.indexOf(".")), properties.getProperty(key));
		}
		for (String sessionName : sessionList) {
			try {
				sessionMap.put(sessionName, buildMailSession(map.get(sessionName)));
			} catch (Exception e) {
				LOGGER.error("", e);
			}
		}
	}

	public static void addMailSession(String sessionName, String host, String port, String username, String password, boolean isSSL) {
		Properties oneProperty = new Properties();
		oneProperty.setProperty("mail.smtp.host", host);
		oneProperty.setProperty("mail.smtp.port", port);
		oneProperty.setProperty("mail.smtp.user", username);
		oneProperty.setProperty("mail.smtp.password", password);
		oneProperty.setProperty("mail.smtp.starttls.enabl", "" + isSSL);
		oneProperty.setProperty("mail.smtp.auth", "true");
		sessionMap.put(sessionName, buildMailSession(oneProperty));
	}

	private static Session buildMailSession(Properties oneProperty) {
		Authenticator authenticator = new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(oneProperty.getProperty("mail.smtp.user"), oneProperty.getProperty("mail.smtp.password"));
			}
		};
		return Session.getInstance(oneProperty, authenticator);
	}

	/**
	 * 发送邮件
	 * 
	 * @param toMails-收件人
	 * @param ccMails-抄送人
	 * @param bccMails-密送人
	 * @param title-邮件标题
	 * @param text-邮件内容
	 * @param attachmentList-附件
	 * @return
	 */
	public static boolean send(String sessionName, String from, String[] toMails, String[] ccMails, String[] bccMails, String title, String text, List<AttachmentDef> attachmentList) {
		try {
			InternetAddress[] toAddresses = new InternetAddress[toMails.length];
			for (int i = 0; i < toMails.length; i++) {
				toAddresses[i] = new InternetAddress(toMails[i]);
			}
			if (ccMails == null)
				ccMails = new String[0];
			InternetAddress[] ccAddresses = new InternetAddress[ccMails.length];
			for (int i = 0; i < ccMails.length; i++) {
				ccAddresses[i] = new InternetAddress(ccMails[i]);
			}
			if (bccMails == null)
				bccMails = new String[0];
			InternetAddress[] bccAddresses = new InternetAddress[bccMails.length];
			for (int i = 0; i < bccMails.length; i++) {
				bccAddresses[i] = new InternetAddress(bccMails[i]);
			}
			MimeMessage message = new MimeMessage(sessionMap.get(sessionName));
			message.setFrom(from);
			message.setSubject(title);
			message.setRecipients(RecipientType.TO, toAddresses);
			message.setRecipients(RecipientType.CC, ccAddresses);
			message.setRecipients(RecipientType.BCC, bccAddresses);
			MimeMultipart bodyMultipart = new MimeMultipart("related");
			message.setContent(bodyMultipart);
			MimeBodyPart htmlBodyPart = new MimeBodyPart();
			bodyMultipart.addBodyPart(htmlBodyPart);
			StringBuilder htmlContent = new StringBuilder(text);
			// 附件
			if (attachmentList != null) {
				htmlContent.append("<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"1280\">");
				String random = CryptoUtil.randomMd5Code();
				for (int i = 0; i < attachmentList.size(); i++) {
					MimeBodyPart attachmentPart = new MimeBodyPart();
					bodyMultipart.addBodyPart(attachmentPart);
					attachmentPart.setDataHandler(new DataHandler(new ByteArrayDataSource(attachmentList.get(i).file, "application/octet-stream")));
					attachmentPart.setFileName(attachmentList.get(i).fileName);
					attachmentPart.setHeader("Content-ID", random + i);
					if (attachmentList.get(i).isImage)
						htmlContent.append("<tr><td><img src=\"cid:" + random + i + "\"/></td></tr>");
				}
				htmlContent.append("</table>");
			}
			htmlBodyPart.setContent(htmlContent.toString(), "text/html;charset=UTF-8");
			// 发送邮件
			message.saveChanges();
			Transport.send(message);
			return true;
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return false;
	}

}
