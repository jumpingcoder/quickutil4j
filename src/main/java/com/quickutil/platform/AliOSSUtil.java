package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 阿里云对象存储工具 官方文档参见：https://help.aliyun.com
 *
 * @author 0.5
 */
public class AliOSSUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliOSSUtil.class);
    private OSSClient ossClient;
    private String bucketName;
    private String accessKeyId;
    private String accessKeySecret;
    private String endPoint;


    public AliOSSUtil(String accessKeyId, String accessKeySecret, String endPoint, String bucketName) {
        ossClient = new OSSClient(endPoint, accessKeyId, accessKeySecret);
        this.bucketName = bucketName;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.endPoint = endPoint;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public OSSClient getOSSClient() {
        return ossClient;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public String getBucketUrl() {
        URI endpointUrl = ossClient.getEndpoint();
        return endpointUrl.getScheme() + Symbol.PROTOCOL + bucketName + Symbol.PERIOD + endpointUrl.getHost();
    }

    /**
     * 获取文件列表
     */
    public List<String> list(String prefix) {
        ObjectListing objectListing = ossClient.listObjects(bucketName, prefix);
        List<String> filePaths = new ArrayList<>();
        List<OSSObjectSummary> filelist = objectListing.getObjectSummaries();
        for (OSSObjectSummary objectSummary : filelist) {
            filePaths.add(objectSummary.getKey());
        }
        return filePaths;
    }

    /**
     * 上传
     */
    public String uploadFile(byte[] bt, String filePath, ObjectMetadata meta) {
        try {
            if (meta == null) {
                meta = new ObjectMetadata();
            }
            InputStream is = new ByteArrayInputStream(bt);
            meta.setContentLength(bt.length);
            ossClient.putObject(bucketName, filePath, is, meta);
            return getBucketUrl() + Symbol.SLASH + filePath;
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
            return null;
        }
    }

    /**
     * 上传整个文件夹
     */
    public List<String> uploadDir(String localPath, String dirPath) {
        List<String> list = new ArrayList<>();
        try {
            int sublength = localPath.lastIndexOf(Symbol.SLASH) + 1;
            List<String> localPaths = FileUtil.getAllFilePath(localPath, null);
            for (String path : localPaths) {
                String filePath = dirPath + path.substring(sublength);
                list.add(uploadFile(FileUtil.file2Byte(path), filePath, null));
            }
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return list;
    }

    /**
     * 下载
     */
    public byte[] downloadFile(String filePath) {
        OSSObject object = ossClient.getObject(bucketName, filePath);
        return FileUtil.stream2byte(object.getObjectContent());
    }

    /**
     * 生成上传链接
     * https://help.aliyun.com/zh/oss/developer-reference/postobject?spm=a2c4g.11186623.0.i12#reference-smp-nsw-wdb
     * base64(hmac-sha1(AccessKeySecret,base64(policy)))
     */
    public String generateUploadSignature(Map<String, Object> policyMap) {
        String base64 = CryptoUtil.byteToBase64(JsonUtil.toJson(policyMap).getBytes());
        return CryptoUtil.byteToBase64(CryptoUtil.HmacSHA1Encrypt(base64.getBytes(StandardCharsets.UTF_8), this.accessKeySecret));
    }

    /**
     * 生成临时下载链接
     */
    public URL generatePresignedUrl(String filePath, int expireSeconds) {
        return ossClient.generatePresignedUrl(bucketName, filePath, new Date(System.currentTimeMillis() + expireSeconds * 1000L));
    }

    /**
     * 删除
     */
    public void deleteFile(String filePath) {
        ossClient.deleteObject(bucketName, filePath);
    }
}
