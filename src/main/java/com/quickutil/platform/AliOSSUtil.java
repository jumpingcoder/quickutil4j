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
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AliOSSUtil {
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliOSSUtil.class);

    private OSSClient ossClient;
    private String bucketName;


    public AliOSSUtil(String accessKeyId, String accessKeySecret, String endPoint, String bucketName) {
        ossClient = new OSSClient(endPoint, accessKeyId, accessKeySecret);
        this.bucketName = bucketName;
    }

    /**
     * 获取文件列表
     *
     * @param prefix-文件前缀
     * @return
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
     *
     * @param bt-文件内容
     * @param filePath-OSS路径
     * @param contentType-文件类型
     * @return
     */
    public String uploadFile(byte[] bt, String filePath, String contentType) {
        try {
            InputStream is = new ByteArrayInputStream(bt);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bt.length);
            if (contentType != null)
                meta.setContentType(contentType);
            ossClient.putObject(bucketName, filePath, is, meta);
            URI endpoint = ossClient.getEndpoint();
            String urlpath = endpoint.getScheme() + "://" + bucketName + "." + endpoint.getHost() + "/" + filePath;
            return urlpath;
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }
    }

    /**
     * 上传整个文件夹
     *
     * @param localPath-本地路径
     * @param dirPath-OSS路径
     * @return
     */
    public List<String> uploadDir(String localPath, String dirPath) {
        List<String> list = new ArrayList<>();
        try {
            int sublength = localPath.lastIndexOf("/") + 1;
            List<String> localPaths = FileUtil.getAllFilePath(localPath, null);
            for (String path : localPaths) {
                String filePath = dirPath + path.substring(sublength);
                String contentType = path.substring(path.lastIndexOf(".") + 1);
                list.add(uploadFile(FileUtil.file2Byte(path), filePath, contentType));
            }
        } catch (Exception e) {
            LOGGER.error("", e);
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
        OSSObject object = ossClient.getObject(bucketName, filePath);
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
        return ossClient.generatePresignedUrl(bucketName, filePath, new Date(System.currentTimeMillis() + expireSeconds * 1000));
    }

    /**
     * 删除
     *
     * @param filePath-文件路径
     */
    public void deleteFile(String filePath) {
        ossClient.deleteObject(bucketName, filePath);
    }
}
