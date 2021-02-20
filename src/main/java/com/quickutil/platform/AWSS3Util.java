package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * AWS对象存储工具
 * 官方文档参见：https://aws.amazon.com/cn/sdk-for-java/
 *
 * @author 0.5
 */
public class AWSS3Util {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AWSS3Util.class);

    private AmazonS3 amazonS3;
    private String bucket;
    private String endPoint;

    public AWSS3Util(String accessKey, String secretKey, String endPoint, String region, String bucket) {
        AmazonS3ClientBuilder s3Builder = AmazonS3ClientBuilder.standard();
        s3Builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        s3Builder.setEndpointConfiguration(new EndpointConfiguration(endPoint, region));
        amazonS3 = s3Builder.build();
        this.bucket = bucket;
        this.endPoint = endPoint;
    }

    /**
     * 获取文件列表
     *
     * @param prefix-文件前缀
     * @return
     */
    public List<String> list(String prefix) {
        List<String> filePaths = new ArrayList<>();
        final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket);
        req.setPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = amazonS3.listObjectsV2(req);
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
     * @param bt-文件内容
     * @param filePath-文件路径
     * @param contentType-文件类型
     */
    public String uploadFile(byte[] bt, String filePath, String contentType, GroupGrantee groupGrantee, Permission permission) {
        InputStream is = new ByteArrayInputStream(bt);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bt.length);
        if (contentType != null)
            metadata.setContentType(contentType);
        if (groupGrantee != null && permission != null) {
            AccessControlList acl = new AccessControlList();
            acl.grantPermission(groupGrantee, permission);
            amazonS3.putObject(new PutObjectRequest(bucket, filePath, is, metadata).withAccessControlList(acl));
        } else {
            amazonS3.putObject(new PutObjectRequest(bucket, filePath, is, metadata));
        }
        return endPoint + "/" + bucket + "/" + filePath;
    }

    /**
     * 上传整个文件夹
     *
     * @param localPath-本地路径
     * @param dirPath-OSS路径
     * @return
     */
    public List<String> uploadDir(String localPath, String dirPath, GroupGrantee groupGrantee, Permission permission) {
        List<String> list = new ArrayList<>();
        try {
            int sublength = localPath.lastIndexOf("/") + 1;
            List<String> localPaths = FileUtil.getAllFilePath(localPath, null);
            for (String path : localPaths) {
                String filePath = dirPath + path.substring(sublength);
                String contentType = path.substring(path.lastIndexOf(".") + 1);
                list.add(uploadFile(FileUtil.file2Byte(path), filePath, contentType, groupGrantee, permission));
            }
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return list;
    }

    /**
     * 下载
     *
     * @param filePath-文件路径
     * @return
     */
    public byte[] downloadFile(String filePath) {
        S3Object object = amazonS3.getObject(bucket, filePath);
        return FileUtil.stream2byte(object.getObjectContent());
    }

    /**
     * 生成临时链接
     *
     * @param filePath-文件路径
     * @param expireSeconds-链接有效时间，秒
     * @return
     */
    public URL generatePresignedUrl(String filePath, int expireSeconds) {
        return amazonS3.generatePresignedUrl(bucket, filePath, new Date(System.currentTimeMillis() + expireSeconds * 1000));
    }

    /**
     * 删除
     *
     * @param filePath-文件路径
     */
    public void deleteFile(String filePath) {
        amazonS3.deleteObject(bucket, filePath);
    }
}
