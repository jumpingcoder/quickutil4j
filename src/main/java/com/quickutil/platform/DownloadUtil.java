package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

public class DownloadUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(DownloadUtil.class);


    /**
     * 下载网络文件
     * @param urlString
     * @param outputPath
     */
    public static void downloadNet(String urlString, String outputPath) {
        try {
            URL url = new URL(urlString);
            URLConnection conn = url.openConnection();
            try(InputStream inStream = conn.getInputStream()){
                FileUtil.writeFile(inStream, outputPath, false);
            }
        } catch (Exception e) {
            LOGGER.error("downloadNet error", e);
        }
    }

}
