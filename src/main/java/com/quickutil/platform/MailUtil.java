/**
 * 邮件发送工具
 * 
 * @class MailUtil
 * @author 0.5
 */
package com.quickutil.platform;

import com.quickutil.platform.def.AttachmentDef;
import java.io.File;
import java.util.List;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.mail.Authenticator;
import javax.mail.Message.RecipientType;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

public class MailUtil {
	private static final String format = "text/html;charset=UTF-8";
	private static Properties mailProperties = null;
	private static Authenticator authenticator = null;

	/**
	 * 初始化
	 * 
	 * @param properties-配置
	 */
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
	public static boolean send(String[] toMails, String[] ccMails, String[] bccMails, String title, String text, List<AttachmentDef> attachmentList) {
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
			Session mailSession = Session.getInstance(mailProperties, authenticator);
			MimeMessage message = new MimeMessage(mailSession);
			message.setFrom(new InternetAddress(mailProperties.getProperty("mail.user")));
			message.setSubject(title);
			message.setContent(text, format);
			message.setRecipients(RecipientType.TO, toAddresses);
			message.setRecipients(RecipientType.CC, ccAddresses);
			message.setRecipients(RecipientType.BCC, bccAddresses);
			if (attachmentList != null) {
				MimeMultipart bodyMultipart = new MimeMultipart("related");// 附件部分
				message.setContent(bodyMultipart);
				MimeBodyPart htmlPart = new MimeBodyPart();// 使用html嵌套
				bodyMultipart.addBodyPart(htmlPart);
				StringBuilder htmlContent = new StringBuilder("<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"1280\">");
				String random = CryptoUtil.randomMd5Code();
				for (int i = 0; i < attachmentList.size(); i++) {
					MimeBodyPart previewPart = new MimeBodyPart();
					bodyMultipart.addBodyPart(previewPart);
					previewPart.setDataHandler(new DataHandler(new ByteArrayDataSource(attachmentList.get(i).file, "application/octet-stream")));
					previewPart.setFileName(attachmentList.get(i).fileName);
					previewPart.setHeader("Content-ID", random + i);
					htmlContent.append("<tr><td><img src=\"cid:" + random + i + "\"/></td></tr>");
				}
				htmlContent.append("</table>");
				htmlPart.setContent(htmlContent.toString(), format);
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

	public static boolean send(String[] toMails, String[] ccMails, String[] bccMails, String title, String text, String[] attachments) {
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
			Session mailSession = Session.getInstance(mailProperties, authenticator);
			MimeMessage message = new MimeMessage(mailSession);
			message.setFrom(new InternetAddress(mailProperties.getProperty("mail.user")));
			message.setSubject(title);
			message.setRecipients(RecipientType.TO, toAddresses);
			message.setRecipients(RecipientType.CC, ccAddresses);
			message.setRecipients(RecipientType.BCC, bccAddresses);
			Multipart multipart = new MimeMultipart();
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setContent(text, "text/html;charset=UTF-8");
			multipart.addBodyPart(messageBodyPart);
			for (String filePath: attachments) {
				MimeBodyPart attachPart = new MimeBodyPart();
				File file = new File(filePath);
				if (!file.exists()) {
					throw new RuntimeException("not exist file: " + filePath);
				}
				attachPart.attachFile(filePath);
				multipart.addBodyPart(attachPart);
			}
			message.setContent(multipart);
			message.saveChanges();
			Transport.send(message);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
