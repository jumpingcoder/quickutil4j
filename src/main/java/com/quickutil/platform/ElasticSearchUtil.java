/**
 * ElasticSearch工具
 * 
 * @class ElasticSearchUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;

public class ElasticSearchUtil {

	private static final String[] replaceArray = { "\t", "\n" };
	private static final String selectIdFormat = "%s/%s/%s/%s/_source";
	private static final String insertFormat = "%s/%s/%s/%s";
	private static final String updateFormat = "%s/%s/%s/%s/_update";
	private static final String deleteIndexFormat = "%s/%s";
	private static final String deleteIdFormat = "%s/%s/%s/%s";
	private static final String indexBulkHead = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
	private static final String updateBulkHead = "{\"update\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
	private static final String deleteBulkHead = "{\"delete\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
	private static final String successLog = "success--/%s/%s/%s--%s";
	private static final String successBatchLog = "success--%s--%s";
	private static final String failLog = "fail--/%s/%s/%s--%s";
	private static final String failreason = "failreason--";
	private static final String failException = "failreason--exception";

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
			HttpResponse response = HttpUtil.httpGet(url, null, null, RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build());
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
			String result = selectByJson(requestUrl, template);
			// System.out.println(result);
			return result;
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
	public static String insert(String host, String index, String type, String id, String source) {
		try {
			long paytime = System.currentTimeMillis();
			String url = String.format(insertFormat, host, index, type, id).replaceAll(" ", "");
			HttpResponse response = HttpUtil.httpPut(url, source.getBytes(), null, null, RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build());
			int statusCode = response.getStatusLine().getStatusCode();
			String responseStr = FileUtil.stream2string(response.getEntity().getContent());
			if (statusCode == 200 || statusCode == 201) {
				System.out.println(String.format(successLog, index, type, id, System.currentTimeMillis() - paytime));
			} else {
				System.out.println(failreason + responseStr);
				System.out.println(String.format(indexBulkHead, index, type, id) + source + "\n");
			}
			return responseStr;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(String.format(indexBulkHead, index, type, id) + source + "\n");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 更新数据
	 *
	 * @param host-请求ES的HOST
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param id-ES的id
	 * @param source-更新的内容
	 * @param isupsert-是否是upsert
	 * @return
	 */
	public static String update(String host, String index, String type, String id, Object source, boolean isupsert) {
		String sourceStr = null;
		try {
			long paytime = System.currentTimeMillis();
			String url = String.format(updateFormat, host, index, type, id).replaceAll(" ", "");
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("doc", source);
			map.put("doc_as_upsert", isupsert);
			sourceStr = JsonUtil.toJson(map);
			HttpResponse response = HttpUtil.httpPost(url, sourceStr.getBytes(), null, null, RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build());
			int statusCode = response.getStatusLine().getStatusCode();
			String responseStr = FileUtil.stream2string(response.getEntity().getContent());
			if (statusCode == 200 || statusCode == 201) {
				System.out.println(String.format(successLog, index, type, id, System.currentTimeMillis() - paytime));
			} else {
				System.out.println(failreason + FileUtil.stream2string(response.getEntity().getContent()));
				System.out.println(String.format(updateBulkHead, index, type, id) + sourceStr + "\n");
			}
			return responseStr;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(String.format(updateBulkHead, index, type, id) + sourceStr + "\n");
			e.printStackTrace();
		}
		return null;
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
	public static String delete(String host, String index, String type, String id) {
		long paytime = System.currentTimeMillis();
		try {
			String url;
			if (id == null)
				url = String.format(deleteIndexFormat, host, index);
			else
				url = String.format(deleteIdFormat, host, index, type, id);
			HttpResponse response = HttpUtil.httpDelete(url, null, null, RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build());
			int statusCode = response.getStatusLine().getStatusCode();
			String responseStr = FileUtil.stream2string(response.getEntity().getContent());
			if (statusCode == 200 || statusCode == 201) {
				System.out.println(String.format(successLog, index, type, id, System.currentTimeMillis() - paytime));
			} else {
				System.out.println(String.format(failLog, index, type, id, System.currentTimeMillis() - paytime));
				System.out.println(failreason + FileUtil.stream2string(response.getEntity().getContent()));
			}
			return responseStr;
		} catch (Exception e) {
			System.out.println(String.format(failLog, index, type, id, System.currentTimeMillis() - paytime));
			e.printStackTrace();
		}
		return null;
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
	private static long lasttime = 0;
	private static StringBuffer sb = new StringBuffer();
	private static int count = 0;

	public static boolean bulkInsertBuffer(String host, String index, String type, String id, String source) {
		long paytime = System.currentTimeMillis();
		String sourceStr = null;
		try {
			sb.append(String.format(indexBulkHead, index, type, id) + source + "\n");
			count++;
			if (System.currentTimeMillis() - lasttime > 1000) {
				sourceStr = sb.toString();
				lasttime = System.currentTimeMillis();
				sb = new StringBuffer();
				System.out.println("content count:" + count);
				count = 0;
				HttpResponse response = HttpUtil.httpPost(host + "/_bulk", sourceStr.getBytes(), null, null,
						RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(60000).build());
				int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode == 200 || statusCode == 201) {
					System.out.println(String.format(successLog, index, type, id, System.currentTimeMillis() - paytime));
				} else {
					System.out.println(failreason + FileUtil.stream2string(response.getEntity().getContent()));
					System.out.println(sourceStr);
				}
			}
			return true;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(sourceStr);
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 批量写入
	 * 
	 * @param host-请求ES的HOST
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-写入的内容，key为id，value为source
	 * @return
	 */
	public static String bulkInsert(String host, String index, String type, Map<String, String> source) {
		long paytime = System.currentTimeMillis();
		String sourceStr = null;
		try {
			StringBuilder bulk = new StringBuilder();
			for (String key : source.keySet()) {
				bulk.append(String.format(indexBulkHead, index, type, key) + source.get(key) + "\n");
			}
			sourceStr = bulk.toString();
			HttpResponse response = HttpUtil.httpPost(host + "/_bulk", sourceStr.getBytes());
			int statusCode = response.getStatusLine().getStatusCode();
			String responseStr = FileUtil.stream2string(response.getEntity().getContent());
			if (statusCode == 200 || statusCode == 201) {
				System.out.println(String.format(successBatchLog, source.size(), System.currentTimeMillis() - paytime));
			} else {
				System.out.println(failreason + FileUtil.stream2string(response.getEntity().getContent()));
				System.out.println(sourceStr);
			}
			return responseStr;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(sourceStr);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 批量更新
	 * 
	 * @param host-请求ES的HOST
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-更新的内容
	 * @param isupsert-是否是upsert
	 * @return
	 */
	public static String bulkUpdate(String host, String index, String type, Map<String, Object> source, boolean isupsert) {
		long paytime = System.currentTimeMillis();
		String sourceStr = null;
		try {
			StringBuilder bulk = new StringBuilder();
			for (String key : source.keySet()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("doc", source.get(key));
				map.put("doc_as_upsert", isupsert);
				bulk.append(String.format(updateBulkHead, index, type, key) + JsonUtil.toJson(map) + "\n");
			}
			sourceStr = bulk.toString();
			HttpResponse response = HttpUtil.httpPost(host + "/_bulk", sourceStr.getBytes());
			int statusCode = response.getStatusLine().getStatusCode();
			String responseStr = FileUtil.stream2string(response.getEntity().getContent());
			if (statusCode == 200 || statusCode == 201) {
				System.out.println(String.format(successBatchLog, source.size(), System.currentTimeMillis() - paytime));
			} else {
				System.out.println(failreason + FileUtil.stream2string(response.getEntity().getContent()));
				System.out.println(sourceStr);
			}
			return responseStr;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(sourceStr);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 批量删除
	 * 
	 * @param host-请求ES的HOST
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param idList-删除的id
	 * @return
	 */
	public static String bulkDelete(String host, List<String> indexList, List<String> typeList, List<String> idList) {
		long paytime = System.currentTimeMillis();
		String sourceStr = null;
		try {
			StringBuilder bulk = new StringBuilder();
			for (int i = 0; i < indexList.size(); i++) {
				bulk.append(String.format(deleteBulkHead, indexList.get(i), typeList.get(i), idList.get(i)));
			}
			sourceStr = bulk.toString();
			HttpResponse response = HttpUtil.httpPost(host + "/_bulk", sourceStr.getBytes());
			int statusCode = response.getStatusLine().getStatusCode();
			String responseStr = FileUtil.stream2string(response.getEntity().getContent());
			if (statusCode == 200 || statusCode == 201) {
				System.out.println(String.format(successBatchLog, idList.size(), System.currentTimeMillis() - paytime));
			} else {
				System.out.println(failreason + FileUtil.stream2string(response.getEntity().getContent()));
				System.out.println(sourceStr);
			}
			return responseStr;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(sourceStr);
			e.printStackTrace();
		}
		return null;
	}
}
