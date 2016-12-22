/**
 * ElasticSearch工具
 * 
 * @class ElasticSearchUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;

public class ElasticSearchUtil {

    private static final String[] replaceArray = { "\t", "\n" };
    private static final String selectIdFormat = "%s/%s/%s/%s/_source";
    private static final String insertFormat = "%s/%s/%s/%s";
    private static final String updateFormat = "%s/%s/%s/%s/_update";
    private static final String deleteFormat = "%s/%s/%s/%s";

    /**
     * 使用模板查询数据
     * 
     * @param requestUrl-请求的ES的URL
     * @param template-查询的DSL模板
     * @param paramMap-参数
     * @return
     */
    public static String selectByTemplate(String requestUrl, String template, Map<String, Object> paramMap) {
        try {
            for (String paramKey : paramMap.keySet()) {
                String json = JsonUtil.toJson(paramMap.get(paramKey));
                template = template.replaceAll("@" + paramKey, json);
            }
            // System.out.println(template);
            for (String replace : replaceArray) {
                template = template.replaceAll(replace, "");
            }
            // System.out.println(result);
            return selectByJson(requestUrl, template);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 使用json查询数据
     * 
     * @param requestUrl-请求的ES的URL
     * @param esjson-查询的DSL
     * @return
     */
    public static String selectByJson(String requestUrl, String esjson) {
        try {
            HttpResponse response = HttpUtil.httpPost(requestUrl, esjson.getBytes());
            return new String(FileUtil.stream2byte(response.getEntity().getContent()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 使用id查询数据
     * 
     * @param host-请求ES的HOST
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @return
     */
    public static String selectById(String host, String index, String type, String id) {
        try {
            String url = String.format(selectIdFormat, host, index, type, id);
            HttpResponse response = HttpUtil.httpGet(url, null, null);
            if (response == null)
                return null;
            if (response.getStatusLine().getStatusCode() == 200)
                return FileUtil.stream2string(response.getEntity().getContent());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 写入数据
     * 
     * @param host-请求ES的HOST
     * @param host-请求ES的HOST
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @param source-写入的内容
     * @return
     */
    public static boolean insert(String host, String index, String type, String id, String source) {
        HttpResponse response = null;
        try {
            long paytime = System.currentTimeMillis();
            String url = String.format(insertFormat, host, index, type, id);
            url = url.replaceAll(" ", "");
            response = HttpUtil.httpPut(url, source.getBytes(), null, null, RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(10000).build());
            if (response == null) {
                System.out.println("insertfail--/" + index + "/" + type + "/" + id + "--" + source);
                System.out.println("failreason--out of time");
                return false;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 201) {
                System.out.println("success--/" + index + "/" + type + "/" + id + "--" + (System.currentTimeMillis() - paytime));
                return true;
            } else {
                System.out.println("insertfail--/" + index + "/" + type + "/" + id + "--" + source);
                System.out.println("failreason--" + FileUtil.stream2string(response.getEntity().getContent()));
            }
        } catch (Exception e) {
            System.out.println("insertfail--/" + index + "/" + type + "/" + id + "--" + source);
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 更新数据
     *
     * @param host-请求ES的HOST
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @param source-更新的内容
     * @return
     */
    public static boolean update(String host, String index, String type, String id, Object source) {
        try {
            long paytime = System.currentTimeMillis();
            String url = String.format(updateFormat, host, index, type, id);
            url = url.replaceAll(" ", "");
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("doc", source);
            HttpResponse response = HttpUtil.httpPost(url, JsonUtil.toJson(map).getBytes(), null, null,
                    RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(10000).build());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 201) {
                System.out.println("success--/" + index + "/" + type + "/" + id + "--" + (System.currentTimeMillis() - paytime));
                return true;
            } else {
                System.out.println("updatefail--/" + index + "/" + type + "/" + id + "--" + source);
                System.out.println("failreason--" + FileUtil.stream2string(response.getEntity().getContent()));
            }
        } catch (Exception e) {
            System.out.println("updatefail--/" + index + "/" + type + "/" + id + "--" + source);
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除数据
     * 
     * @param host-请求ES的HOST
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @return
     */
    public static boolean delete(String host, String index, String type, String id) {
        try {
            long paytime = System.currentTimeMillis();
            String url = String.format(deleteFormat, host, index, type, id);
            url = url.replaceAll("/null", "");
            HttpResponse response = HttpUtil.httpDelete(url, null, null, RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(10000).build());
            if (response.getStatusLine().getStatusCode() == 200) {
                System.out.println("success--/" + index + "/" + type + "/" + id + "--" + (System.currentTimeMillis() - paytime));
                return true;
            } else {
                System.out.println("deletefail--/" + index + "/" + type + "/" + id);
                System.out.println("failreason--" + FileUtil.stream2string(response.getEntity().getContent()));
            }
        } catch (Exception e) {
            System.out.println("deletefail--/" + index + "/" + type + "/" + id);
            e.printStackTrace();
        }
        return false;
    }

    private static final String info = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
    private static long lasttime = 0;
    private static StringBuffer sb = new StringBuffer();
    private static int count = 0;

    /**
     * 批量写入
     * 
     * @param host-请求ES的HOST
     * @param index-ES的index
     * @param type-ES的type
     * @param source-写入的内容，key为id，value为source
     * @return
     */
    public static boolean bulkInsert(String host, String index, String type, Map<String, String> source) {
        String result = null;
        try {
            StringBuilder bulk = new StringBuilder();
            for (String key : source.keySet()) {
                bulk.append(String.format(info, index, type, key) + source.get(key) + "\n");
            }
            result = bulk.toString();
            HttpResponse response = HttpUtil.httpPost(host + "/_bulk", result.getBytes());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 201) {
                System.out.println("success--" + source.size());
                return true;
            } else {
                System.out.println("failreason--" + FileUtil.stream2string(response.getEntity().getContent()));
                System.out.println(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("failreason--exception");
            System.out.println(result);
        }
        return false;
    }

    /**
     * 缓存队列式批量写入，每秒写入一次
     * 
     * @param host-请求ES的HOST
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @param source-写入的内容
     * @return
     */
    public static boolean insertByBulkBuffer(String host, String index, String type, String id, String source) {
        String result = null;
        try {
            sb.append(String.format(info, index, type, id) + source + "\n");
            count++;
            if (System.currentTimeMillis() - lasttime > 1000) {
                result = sb.toString();
                lasttime = System.currentTimeMillis();
                sb = new StringBuffer();
                System.out.println("content count:" + count);
                count = 0;
                HttpResponse response = HttpUtil.httpPost(host + "/_bulk", result.getBytes());
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 || statusCode == 201) {
                    System.out.println("success--/" + index + "/" + type + "/" + id);
                    return true;
                } else {
                    System.out.println("failreason--" + FileUtil.stream2string(response.getEntity().getContent()));
                    System.out.println(result);
                }
            } else {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("failreason--exception");
            System.out.println(result);
        }
        return false;
    }

}
