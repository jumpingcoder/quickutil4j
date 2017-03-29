/**
 * 注释生成文档工具
 * 
 * @class AnnotationUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.markdownj.MarkdownProcessor;

public class AnnotationUtil {

    private static final String header = "<html><head><style id=\"system\" type=\"text/css\">h1,h2,h3,h4,h5,h6,p,blockquote {    margin: 0;    padding: 0;}body {    font-family: \"Helvetica Neue\", Helvetica, \"Hiragino Sans GB\", Arial, sans-serif;    font-size: 13px;    line-height: 18px;    color: #737373;    margin: 10px 13px 10px 13px;}a {    color: #0069d6;}a:hover {    color: #0050a3;    text-decoration: none;}a img {    border: none;}p {    margin-bottom: 9px;}h1,h2,h3,h4,h5,h6 {    color: #404040;    line-height: 36px;}h1 {    margin-bottom: 18px;    font-size: 30px;}h2 {    font-size: 24px;}h3 {    font-size: 18px;}h4 {    font-size: 16px;}h5 {    font-size: 14px;}h6 {    font-size: 13px;}hr {    margin: 0 0 19px;    border: 0;    border-bottom: 1px solid #ccc;}blockquote {    padding: 13px 13px 21px 15px;    margin-bottom: 18px;    font-family:georgia,serif;    font-style: italic;}blockquote:before {    content:\"C\";    font-size:40px;    margin-left:-10px;    font-family:georgia,serif;    color:#eee;}blockquote p {    font-size: 14px;    font-weight: 300;    line-height: 18px;    margin-bottom: 0;    font-style: italic;}code, pre {    font-family: Monaco, Andale Mono, Courier New, monospace;}code {    background-color: #fee9cc;    color: rgba(0, 0, 0, 0.75);    padding: 1px 3px;    font-size: 12px;    -webkit-border-radius: 3px;    -moz-border-radius: 3px;    border-radius: 3px;}pre {    display: block;    padding: 14px;    margin: 0 0 18px;    line-height: 16px;    font-size: 11px;    border: 1px solid #d9d9d9;    white-space: pre-wrap;    word-wrap: break-word;}pre code {    background-color: #fff;    color:#737373;    font-size: 11px;    padding: 0;}@media screen and (min-width: 768px) {    body {        width: 748px;        margin:10px auto;    }}</style><style id=\"custom\" type=\"text/css\"></style><style id=\"hibot\" type=\"text/css\">.hibot  { padding:0 0 0 38px; margin:auto; font-size:14px;font-family: monospace; background:#111; color: #e6e1dc;  }.hibot li{ margin: 0; padding-left: 10px; border-left: 2px solid #ccc; background:#111; list-style-position: outside; list-style-type: decimal; text-indent: 0; word-wrap: break-word; word-break: break-all; }.hibot .num { color: #a5c261; }.hibot .attr { color : #4093CC} .hibot .comment { color: #bc9458; font-style: italic; }.hibot .special { color: #da4939; }.hibot .statement { color: #cc7833; }.hibot .preProc { color: #e6e1dc; }.hibot .include, .hibot .head { color: #cc7833; }.hibot .string { color: #a5c261; }.hibot .type, .hibot .keyword, .hibot .val { color: #da4939; }</style></head>";

    /**
     * 将文件夹内的注释生成swagger.json
     * 
     * @param inputDic-输入的文件夹
     * @param outputFile-输出的文件路径
     * @param host-请求的host
     * @param isHttps-是否是https
     **/
    public static void annotationToSwagger(String inputDic, String outputFile, String host, boolean isHttps) {
        List<String> fileList = FileUtil.getAllFilePath(inputDic, null);
        Map<String, Map<String, Object>> pathMap = new HashMap<String, Map<String, Object>>();
        for (String filePath : fileList) {
            try {
                String content = FileUtil.file2String(filePath);
                File file = new File(filePath);
                toSwagger(file.getName().split("\\.")[0], content, pathMap);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(filePath);
            }
        }
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("basePath", "");
        resultMap.put("swagger", "2.0");
        resultMap.put("host", host);
        if (isHttps)
            resultMap.put("schemes", Arrays.asList("https"));
        else
            resultMap.put("schemes", Arrays.asList("http"));
        resultMap.put("paths", pathMap);
        FileUtil.string2File(outputFile, JsonUtil.toJson(resultMap), false);
    }

    private static final String[] paramArray = { "@path", "@header", "@body", "@param", "@query", "@formData" };

    private static String getParamType(String param) {
        for (String p : paramArray) {
            if (param.contains(p))
                return p;
        }
        return null;
    }

    private static void toSwagger(String tag, String content, Map<String, Map<String, Object>> pathMap) {
        String[] contentList = content.split("\n");
        for (int x = 0; x < contentList.length - 3; x++) {
            if (!contentList[x].endsWith("/**") || !contentList[x + 3].contains("@controller"))
                continue;
            String summary = contentList[x + 1].substring(contentList[x + 1].indexOf("*") + 2);
            String controller = null;
            String method = null;
            String consume = null;
            Map<String, Object> methodMap = new HashMap<String, Object>();
            List<Map<String, Object>> paramsList = new ArrayList<Map<String, Object>>();
            for (int i = x; i < contentList.length; i++) {
                if (contentList[i].contains("@controller")) {
                    controller = contentList[i].substring(contentList[i].indexOf("@controller") + "@controller".length() + 1);
                }
                if (contentList[i].contains("@method")) {
                    method = contentList[i].substring(contentList[i].indexOf("@method") + "@method".length() + 1);
                }
                if (contentList[i].contains("@contentType")) {
                    consume = contentList[i].substring(contentList[i].indexOf("@contentType") + "@contentType".length() + 1);
                }
                String paramType = getParamType(contentList[i]);
                if (paramType != null) {
                    String in = paramType.substring(1);
                    if (in.equals("param")) {
                        if (method.equals("GET"))
                            in = "query";
                        else
                            in = "formData";
                    }
                    String line = contentList[i].substring(contentList[i].indexOf(paramType) + paramType.length() + 1);
                    String[] lines = line.split("-");
                    Map<String, Object> paramMap = new HashMap<String, Object>();
                    paramMap.put("in", in);
                    paramMap.put("name", lines[0]);
                    paramMap.put("type", lines[1]);
                    if (lines[2].equals("required"))
                        paramMap.put("required", true);
                    else
                        paramMap.put("required", false);
                    paramMap.put("description", lines[3]);
                    paramsList.add(paramMap);
                }
                if (contentList[i].contains("public") || contentList[i].contains("protected") || contentList[i].contains("private")) {
                    methodMap.put("summary", summary);
                    methodMap.put("parameters", paramsList);
                    methodMap.put("consumes", Arrays.asList(consume));
                    methodMap.put("produces", Arrays.asList("application/json"));
                    methodMap.put("operationId", tag);
                    methodMap.put("description", "");
                    methodMap.put("tags", Arrays.asList(tag));
                    if (pathMap.get(controller) == null)
                        pathMap.put(controller, new HashMap<String, Object>());
                    pathMap.get(controller).put(method.toLowerCase(), methodMap);
                    break;
                }
            }

        }
    }
    
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
                e.printStackTrace();
                System.out.println(filePath);
            }
        }
    }

    /**
     * 注释转为MarkDown
     * 
     * @param content-代码文件
     * @return
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
     * @return
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
