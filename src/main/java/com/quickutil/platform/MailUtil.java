package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.entity.AttachmentDef;
import com.quickutil.platform.constants.Symbol;
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
import java.util.List;
import java.util.Properties;

/**
 * 邮件发送工具
 *
 * @author 0.5
 */
public class MailUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MailUtil.class);

    private Session session;

    public MailUtil(String host, String port, String username, String password, boolean isSSL) {
        session = buildMailSession(host, port, username, password, isSSL);
    }

    private Session buildMailSession(String host, String port, String username, String password, boolean isSSL) {
        Properties mailProperties = new Properties();
        mailProperties.setProperty("mail.smtp.host", host);
        mailProperties.setProperty("mail.smtp.port", port);
        mailProperties.setProperty("mail.smtp.auth", "true");
        if (isSSL) {
            mailProperties.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            mailProperties.setProperty("mail.smtp.socketFactory.port", port);
        }
        Authenticator authenticator = new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        };
        return Session.getInstance(mailProperties, authenticator);
    }

    /**
     * 发送邮件
     *
     * @param from-发件人
     * @param toMails-收件人
     * @param ccMails-抄送人
     * @param bccMails-密送人
     * @param title-邮件标题
     * @param text-邮件内容
     * @param attachmentList-附件
     * @return
     */
    public boolean send(String from, String[] toMails, String[] ccMails, String[] bccMails, String title, String text, List<AttachmentDef> attachmentList) {
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
            MimeMessage message = new MimeMessage(session);
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
            LOGGER.error(Symbol.BLANK, e);
        }
        return false;
    }

}
