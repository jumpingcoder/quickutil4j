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

public class AliSmsUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliSmsUtil.class);

    private IAcsClient client;
    private String templateCode;
    private String signName;

    public AliSmsUtil(String accessKey, String accessSecret, String templateCode, String SignName) {
        DefaultProfile profile = DefaultProfile.getProfile("default", accessKey, accessSecret);
        client = new DefaultAcsClient(profile);
        this.templateCode = templateCode;
        this.signName = SignName;
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
        }
        CommonRequest request = new CommonRequest();
        //request.setProtocol(ProtocolType.HTTPS);
        request.setMethod(MethodType.POST);
        request.setDomain("dysmsapi.aliyuncs.com");
        request.setVersion("2017-05-25");
        request.setAction("SendBatchSms");
        request.putQueryParameter("PhoneNumberJson", JsonUtil.toJson(phoneNumbers));
        request.putQueryParameter("TemplateCode", "SMS_1955290");
        request.putQueryParameter("SignNameJson", JsonUtil.toJson(signNames));
        request.putQueryParameter("TemplateParamJson", JsonUtil.toJson(list));
        try {
            CommonResponse response = client.getCommonResponse(request);
            return response.getData();
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }
}
