package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class AliOSSPoolUtil {
	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliOSSPoolUtil.class);

	private static Map<String, Map<String, String>> bucketMap = new HashMap<String, Map<String, String>>();

	/**
	 * 初始化
	 * 
	 * @param properties-配置
	 * @return
	 */
	public static boolean init(Properties properties) {
		Enumeration<?> keys = properties.propertyNames();
		Set<String> keyList = new HashSet<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			keyList.add(key);
		}
		for (String key : keyList) {
			try {
				Map<String, String> map = new HashMap<String, String>();
				map.put("accessKeyId", properties.getProperty(key + ".accessKeyId"));
				map.put("accessKeySecret", properties.getProperty(key + ".accessKeySecret"));
				map.put("endpoint", properties.getProperty(key + ".endpoint"));
				map.put("bucketname", properties.getProperty(key + ".bucketname"));
				bucketMap.put(key, map);
			} catch (Exception e) {
				LOGGER.error("", e);
			}
		}
		return true;
	}

	/**
	 * 获取客户端实例
	 * 
	 * @param ossName-ossName
	 * @return
	 */
	public static OSSClient buildClient(String ossName) {
		return new OSSClient(bucketMap.get(ossName).get("endpoint"), bucketMap.get(ossName).get("accessKeyId"), bucketMap.get(ossName).get("accessKeySecret"));
	}

	/**
	 * 获取文件列表
	 * 
	 * @param ossName-ossName
	 * @param prefix-文件前缀
	 * @return
	 */
	public static List<String> list(String ossName, String prefix) {
		ObjectListing objectListing = buildClient(ossName).listObjects(bucketMap.get(ossName).get("bucketname"), prefix);
		List<String> filePaths = new ArrayList<String>();
		List<OSSObjectSummary> filelist = objectListing.getObjectSummaries();
		for (OSSObjectSummary objectSummary : filelist) {
			filePaths.add(objectSummary.getKey());
		}
		return filePaths;
	}

	/**
	 * 上传
	 * 
	 * @param ossName-ossName
	 * @param bt-文件内容
	 * @param filePath-文件路径
	 * @param contentType-文件类型
	 * @return
	 */
	public static String uploadFile(String ossName, byte[] bt, String filePath, String contentType) {
		try {
			InputStream is = new ByteArrayInputStream(bt);
			ObjectMetadata meta = new ObjectMetadata();
			meta.setContentLength(bt.length);
			if (contentType != null)
				meta.setContentType(contentType);
			buildClient(ossName).putObject(bucketMap.get(ossName).get("bucketname"), filePath, is, meta);
			String endpoint = bucketMap.get(ossName).get("endpoint");
			int index = endpoint.indexOf("/") + 2;
			endpoint = endpoint.substring(0, index) + bucketMap.get(ossName).get("bucketname") + "." + endpoint.substring(index);
			String urlpath = endpoint + "/" + filePath;
			return urlpath;
		} catch (Exception e) {
			LOGGER.error("", e);
			return null;
		}
	}

	/**
	 * 下载
	 * 
	 * @param ossName-ossName
	 * @param filePath-文件路径
	 * @return
	 */
	public static byte[] downloadFile(String ossName, String filePath) {
		OSSObject object = buildClient(ossName).getObject(bucketMap.get(ossName).get("bucketname"), filePath);
		return FileUtil.stream2byte(object.getObjectContent());
	}

	/**
	 * 生成临时链接
	 * 
	 * @param ossName-ossName
	 * @param filePath-文件路径
	 * @param expireSeconds-链接有效时间，秒
	 * @return
	 * 
	 */
	public static URL generatePresignedUrl(String ossName, String filePath, int expireSeconds) {
		return buildClient(ossName).generatePresignedUrl(bucketMap.get(ossName).get("bucketname"), filePath, new Date(System.currentTimeMillis() + expireSeconds * 1000));
	}

	/**
	 * 删除
	 * 
	 * @param ossName-ossName
	 * @param filePath-文件路径
	 */
	public static void deleteFile(String ossName, String filePath) {
		buildClient(ossName).deleteObject(bucketMap.get(ossName).get("bucketname"), filePath);
	}
}
