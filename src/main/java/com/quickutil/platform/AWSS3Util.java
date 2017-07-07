package com.quickutil.platform;

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AWSS3Util {

	private static Map<String, Map<String, String>> bucketMap = new HashMap<String, Map<String, String>>();

	/**
	 * 初始化
	 * 
	 * @param properties
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
				map.put("region", properties.getProperty(key + ".region"));
				map.put("bucketname", properties.getProperty(key + ".bucketname"));
				bucketMap.put(key, map);
			} catch (Exception e) {
				LogUtil.error(e, "S3配置参数错误");
			}
		}
		return true;
	}

	/**
	 * 获取客户端实例
	 * 
	 * @param s3Name
	 * @return
	 */
	public static AmazonS3 buildClient(String s3Name) {
		AmazonS3ClientBuilder s3Builder = AmazonS3ClientBuilder.standard();
		s3Builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(bucketMap.get(s3Name).get("accessKeyId"), bucketMap.get(s3Name).get("accessKeySecret"))));
		s3Builder.setEndpointConfiguration(new EndpointConfiguration(bucketMap.get(s3Name).get("endpoint"), bucketMap.get(s3Name).get("region")));
		return s3Builder.build();
	}

	/**
	 * 获取文件列表
	 * 
	 * @param s3Name
	 * @param prefix
	 * @return
	 */
	public static List<String> list(String s3Name, String prefix) {
		List<String> filePaths = new ArrayList<String>();
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketMap.get(s3Name).get("bucketname")).withMaxKeys(100);
		ListObjectsV2Result result;
		do {
			result = buildClient(s3Name).listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				filePaths.add(objectSummary.getKey());
			}
			req.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated());
		return filePaths;

	}

	/**
	 * 上传
	 * 
	 * @param s3Name
	 * @param bt
	 * @param filePath
	 * @param contentType
	 */
	public static String uploadFile(String s3Name, byte[] bt, String filePath, String contentType) {
		InputStream is = new ByteArrayInputStream(bt);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(bt.length);
		if (contentType != null)
			metadata.setContentType(contentType);
		AccessControlList acl = new AccessControlList();
		acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
		buildClient(s3Name).putObject(new PutObjectRequest(bucketMap.get(s3Name).get("bucketname"), filePath, is, metadata).withAccessControlList(acl));
		return bucketMap.get(s3Name).get("endpoint") + "/" + bucketMap.get(s3Name).get("bucketname") + "/" + filePath;
	}

	/**
	 * 删除
	 * 
	 * @param s3Name
	 * @param filePath
	 */
	public static void deleteFile(String s3Name, String filePath) {
		buildClient(s3Name).deleteObject(bucketMap.get(s3Name).get("bucketname"), filePath);
	}
}
