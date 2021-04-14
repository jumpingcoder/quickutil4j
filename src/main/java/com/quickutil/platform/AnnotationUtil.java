package com.quickutil.platform;

import com.quickutil.platform.constants.Symbol;
import org.markdownj.MarkdownProcessor;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

import java.util.*;

/**
 * 注释生成文档工具
 *
 * @author 0.5
 */
@Deprecated
public class AnnotationUtil {
	
	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AnnotationUtil.class);

	private static final String header = "<html><head><style id=\"system\" type=\"text/css\">h1,h2,h3,h4,h5,h6,p,blockquote {    margin: 0;    padding: 0;}body {    font-family: \"Helvetica Neue\", Helvetica, \"Hiragino Sans GB\", Arial, sans-serif;    font-size: 13px;    line-height: 18px;    color: #737373;    margin: 10px 13px 10px 13px;}a {    color: #0069d6;}a:hover {    color: #0050a3;    text-decoration: none;}a img {    border: none;}p {    margin-bottom: 9px;}h1,h2,h3,h4,h5,h6 {    color: #404040;    line-height: 36px;}h1 {    margin-bottom: 18px;    font-size: 30px;}h2 {    font-size: 24px;}h3 {    font-size: 18px;}h4 {    font-size: 16px;}h5 {    font-size: 14px;}h6 {    font-size: 13px;}hr {    margin: 0 0 19px;    border: 0;    border-bottom: 1px solid #ccc;}blockquote {    padding: 13px 13px 21px 15px;    margin-bottom: 18px;    font-family:georgia,serif;    font-style: italic;}blockquote:before {    content:\"C\";    font-size:40px;    margin-left:-10px;    font-family:georgia,serif;    color:#eee;}blockquote p {    font-size: 14px;    font-weight: 300;    line-height: 18px;    margin-bottom: 0;    font-style: italic;}code, pre {    font-family: Monaco, Andale Mono, Courier New, monospace;}code {    background-color: #fee9cc;    color: rgba(0, 0, 0, 0.75);    padding: 1px 3px;    font-size: 12px;    -webkit-border-radius: 3px;    -moz-border-radius: 3px;    border-radius: 3px;}pre {    display: block;    padding: 14px;    margin: 0 0 18px;    line-height: 16px;    font-size: 11px;    border: 1px solid #d9d9d9;    white-space: pre-wrap;    word-wrap: break-word;}pre code {    background-color: #fff;    color:#737373;    font-size: 11px;    padding: 0;}@media screen and (min-width: 768px) {    body {        width: 748px;        margin:10px auto;    }}</style><style id=\"custom\" type=\"text/css\"></style><style id=\"hibot\" type=\"text/css\">.hibot  { padding:0 0 0 38px; margin:auto; font-size:14px;font-family: monospace; background:#111; color: #e6e1dc;  }.hibot li{ margin: 0; padding-left: 10px; border-left: 2px solid #ccc; background:#111; list-style-position: outside; list-style-type: decimal; text-indent: 0; word-wrap: break-word; word-break: break-all; }.hibot .num { color: #a5c261; }.hibot .attr { color : #4093CC} .hibot .comment { color: #bc9458; font-style: italic; }.hibot .special { color: #da4939; }.hibot .statement { color: #cc7833; }.hibot .preProc { color: #e6e1dc; }.hibot .include, .hibot .head { color: #cc7833; }.hibot .string { color: #a5c261; }.hibot .type, .hibot .keyword, .hibot .val { color: #da4939; }</style></head>";

	/**
	 * 注释转为MarkDown和Html
	 *
	 * @param inputDic-输入的文件夹
	 * @param outputDic-输出的文件夹
	 */
	public static void annotationToDoc(String inputDic, String outputDic) {
		List<String> fileList = FileUtil.getAllFilePath(inputDic, null);
		for (String filePath : fileList) {
			try {
				String content = FileUtil.file2String(filePath);
				String mdContent = annotationToMarkDown(content);
				String mdPath = outputDic + "/markdown" + filePath.replaceAll(inputDic, "").replaceAll("\\.java", ".md");
				FileUtil.string2File(mdPath, mdContent, false);
				String htmlPath = outputDic + "/html" + filePath.replaceAll(inputDic, "").replaceAll("\\.java", ".html");
				String htmlContent = markdownToHtml(mdContent);
				FileUtil.string2File(htmlPath, htmlContent, false);
			} catch (Exception e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}

	/**
	 * 注释转为MarkDown
	 *
	 * @param content-代码文件
	 * @return markdown格式的字符串
	 */
	public static String annotationToMarkDown(String content) throws Exception {
		StringBuilder sb = new StringBuilder();
		String[] contentList = content.split("\n");
		boolean in = false;
		String title = null;
		StringBuilder sbin = new StringBuilder();
		for (int i = 0; i < contentList.length; i++) {
			if (contentList[i].endsWith("/**")) {
				in = true;
				title = contentList[i + 1].substring(contentList[i + 1].indexOf("*") + 2);
				continue;
			}
			if (in) {
				if (contentList[i].contains("@class")) {
					sb.append("## " + title + "\n\n");
					sb.append("#### " + contentList[i].substring(contentList[i].indexOf("*") + 2) + "\n\n");
					sb.append("#### " + contentList[i + 1].substring(contentList[i + 1].indexOf("*") + 2) + "\n\n");
					in = false;
					title = null;
				}
				if (contentList[i].contains("@param")) {
					String line = contentList[i].substring(contentList[i].indexOf("@param") + 6);
					String[] array = line.split("-");
					sbin.append("+" + array[0] + ": " + line.substring(array[0].length() + 1) + "\n");
				}
				if (contentList[i].contains("public") || contentList[i].contains("protected") || contentList[i].contains("private")) {
					sb.append("#### " + title + "\n\n");
					sb.append("```java\n");
					sb.append(contentList[i].substring(contentList[i].indexOf("p")).replaceAll("\\{", "") + "\n");
					sb.append("```\n");
					sb.append(sbin.toString() + "\n");
					in = false;
					title = null;
					sbin = new StringBuilder();
				}
			}
		}
		return sb.toString();
	}

	/**
	 * MarkDown转为Html
	 *
	 * @param content-MarkDown文件
	 * @return html格式的字符串
	 */
	public static String markdownToHtml(String content) {
		content = content.replaceAll("```java", "");
		content = content.replaceAll("```", "");
		content = content.replaceAll("_", "##1001");
		content = content.replaceAll("<", "&lt;");
		content = content.replaceAll(">", "&gt;");
		content = new MarkdownProcessor().markdown(content);
		content = content.replaceAll("##1001", "_");
		content = header + content + "</html>";
		return content;
	}
}
