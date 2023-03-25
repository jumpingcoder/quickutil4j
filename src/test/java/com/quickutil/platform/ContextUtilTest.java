package com.quickutil.platform;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.api.client.json.Json;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.quickutil.platform.constants.Symbol;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HTTP;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;


/**
 * ContextUtilTest
 *
 * @author 0.5
 */

public class ContextUtilTest {

    static {
        Set<String> loggers = new HashSet<>(Arrays.asList("org.apache.http", "groovyx.net.http"));
        for (String log : loggers) {
            Logger logger = (Logger) LoggerFactory.getLogger(log);
            logger.setLevel(Level.INFO);
            logger.setAdditive(false);
        }
    }

    @Test
    public void getOriginalUrl() {
        Assert.assertEquals("https://abc:123@quickutil.com:443/hello?xx=123#part1", ContextUtil.getOriginalUrl("http://abc:123@vndjsak.com:443/hello?xx=123#part1", "https", "quickutil.com"));
        Assert.assertEquals("https://quickutil.com/#part1", ContextUtil.getOriginalUrl("http://vndjsak.com/#part1", "https", "quickutil.com"));
        Assert.assertEquals("https://quickutil.com", ContextUtil.getOriginalUrl("http://vndjsak.com", "https", "quickutil.com"));
    }

}