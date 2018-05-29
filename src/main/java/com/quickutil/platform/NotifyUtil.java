package com.quickutil.platform;

import com.dji.dava.gateway.GatewayClient;
import com.dji.dava.gateway.Response;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;


public class NotifyUtil {
    private static Gson gson = new Gson();
    private static GatewayClient client;

    public static void init(String gwUrl, String appId, String appSecret){
        client = new GatewayClient(gwUrl, appId, appSecret);
    }

    public static Response sendGT(String toAD, String message) throws Exception {
        return sendGT(toAD, message, new HashMap<>());
    }

    public static Response sendGT(String toAD, String message, Map<String, String> headers) throws Exception{
        Map<String, String> formData = new HashMap<String, String>();
        formData.put("appId", "data-d");
        formData.put("toad", toAD);
        formData.put("content", message);
        return client.httpPost("dji-ums.send.gt.post", null, headers, gson.toJson(formData), "json");
    }

    public static Response sendEmail(String[] recipients, String title, String content) throws Exception {
        return sendEmail(recipients, title, content, new HashMap<>());
    }

    public static Response sendEmail(String[] recipients, String title, String content, Map<String, String> header) throws Exception {
        Map<String, Object> formData = new HashMap<>();
        formData.put("appId", "data-d");
        formData.put("title", title);
        formData.put("recipients", recipients);
        formData.put("content", content);
        return client.httpPost("dji-ums.send.rawemail.post", null, header, gson.toJson(formData), "json");
    }
}
