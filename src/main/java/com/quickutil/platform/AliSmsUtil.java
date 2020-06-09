package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 阿里云大于发送短信工具
 * 官方文档参见：https://help.aliyun.com
 *
 * @author 0.5
 */
public class AliSmsUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliSmsUtil.class);

    private IAcsClient client;
    private String templateCode;
    private String signName;

    public AliSmsUtil(String accessKey, String accessSecret, String templateCode, String signName) {
        DefaultProfile profile = DefaultProfile.getProfile("default", accessKey, accessSecret);
        client = new DefaultAcsClient(profile);
        this.templateCode = templateCode;
        this.signName = signName;
    }

    public String send(List<Map<String, Object>> list) {
        List<String> phoneNumbers = new ArrayList<>();
        List<String> signNames = new ArrayList<>();
        for (Map<String, Object> map : list) {
            if (map.get("phoneNumber") == null) {
                LOGGER.error("入参必须要有phoneNumber");
                return null;
            }
            phoneNumbers.add((String) map.get("phoneNumber"));
            signNames.add(signName);
            map.remove("phoneNumber");
        }
        CommonRequest request = new CommonRequest();
        //request.setProtocol(ProtocolType.HTTPS);
        request.setMethod(MethodType.POST);
        request.setDomain("dysmsapi.aliyuncs.com");
        request.setVersion("2017-05-25");
        request.setAction("SendBatchSms");
        LOGGER.info(JsonUtil.toJson(phoneNumbers));
        LOGGER.info(JsonUtil.toJson(signNames));
        LOGGER.info(JsonUtil.toJson(list));
        request.putQueryParameter("PhoneNumberJson", JsonUtil.toJson(phoneNumbers));
        request.putQueryParameter("TemplateCode", this.templateCode);
        request.putQueryParameter("SignNameJson", JsonUtil.toJson(signNames));
        request.putQueryParameter("TemplateParamJson", JsonUtil.toJson(list));
        try {
            CommonResponse response = client.getCommonResponse(request);
            return response.getData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
