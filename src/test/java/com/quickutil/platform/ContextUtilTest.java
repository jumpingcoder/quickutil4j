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

    @Test
    public void cleanES() {
        try {
            HttpResponse response = HttpUtil.httpGet("http://10.129.104.11:9218/_cat/indices");
            String content = FileUtil.stream2string(response.getEntity().getContent());
            String[] lines = content.split("\n");
            for (String line : lines) {
                if (line.contains("green open") && line.contains("2021") && !line.contains("202112")) {
                    String index = line.replaceAll("green open  ", "");
                    index = index.substring(0, index.indexOf(" "));
                    response = HttpUtil.httpPost("http://10.129.104.11:9218/" + index + "/_close");
                    System.out.println(index + ":" + response.getStatusLine().getStatusCode());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Map<String, Object> parseStruct(String src) {
        Map<String, Object> result = new HashMap<>();
        String[] lines = src.split("\t");
        for (String line : lines) {
            //part1
            if (line.startsWith("cmd")) {
                String[] cmdArray = line.split(",");
                for (String cmdS : cmdArray) {
//                    System.out.println(cmdS);
                    String[] cmdKv = cmdS.split(":");
                    if (cmdKv.length == 2)
                        result.put(cmdKv[0], cmdKv[1]);
                }
            }
            //part2
            else if (line.startsWith("{")) {
                //System.out.println(s);
                Map<String, Object> content = JsonUtil.toMap(line);
                for (String k : content.keySet()) {
                    if (k.startsWith("biz_content")) {
                        try {
                            result.put(k, JsonUtil.toJsonObject(content.get(k).toString()));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        result.put(k, content.get(k));
                    }
                }
            }
            //part3
            else {
//                System.out.println(s);
                if (!line.contains(":"))
                    continue;
                String[] kv = line.split(":");
                result.put(kv[0], kv[1]);
            }
        }
        return result;
    }

    @Test
    public void kafkaToES() throws Exception {
//        String src = "CreateTime:1652898249936\tPartition:63\tOffset:4194\tcmd:SERVER_SEND,biz_id:1,biz_type:SEND_MSG,user_id:0,timestamp:1652898249,request_id:1526992051708928000\t{\"gw_req_id\":\"1526992051708928001\",\"biz_content\":\"{\\\"session_id\\\":\\\"1526977923470073856\\\",\\\"msg_type\\\":\\\"ABOUT_TO_SESSION_TIMEOUT\\\",\\\"msg_id\\\":\\\"\\\",\\\"seq_id\\\":\\\"1526992051666984960\\\",\\\"content\\\":\\\"{\\\\\\\"remain_time\\\\\\\":60}\\\",\\\"user_type\\\":0,\\\"ctime\\\":1652898249,\\\"region\\\":\\\"\\\",\\\"user_id\\\":0}\",\"no_ack\":true,\"to\":[{\"biz_id\":1,\"user_ids\":[380610153682907]}]}";
        String headerTemplate = "{ \"index\" : { \"_index\" : \"test_br_topic_msg\", \"_id\" : \"%s\" } }";
        List<String> list = FileUtil.readFileByLine("/Users/miles.miao/Downloads/msg.txt");
        Map<String, String> httpHeader = new HashMap<>();
        httpHeader.put("content-type", "application/json");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> result = parseStruct(list.get(i));
            String header = String.format(headerTemplate, result.get("Partition").toString() + "_" + result.get("Offset").toString());
            sb.append(header + "\n" + JsonUtil.toJson(result) + "\n");
            if (i % 1000 == 0 || i == list.size() - 1) {
                HttpResponse response = HttpUtil.httpPost("http://10.129.104.11:9218/_bulk", sb.toString().getBytes(), httpHeader);
//                System.out.println(FileUtil.stream2string(response.getEntity().getContent()));
                sb = new StringBuilder();
                System.out.println(i);
            }
//            FileUtil.string2File("/Users/miles.miao/Downloads/msg_to_es.txt", header + "\n" + JsonUtil.toJson(result) + "\n", true);
        }
    }

    @Test
    public void getHiveTables() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("cookie", "_ga_QMDMFHB4T7=GS1.1.1653533438.12.0.1653533438.0; CSRF-VERIFY-TOKEN=f706ba9c-8392-422a-b98c-2d20b59fa1dd; CSRF-TOKEN=29aab767fc6590997f8f7cebef682072af26592bbfe85f19ceb52853f6ac99fd; G_AUTHUSER_H=0; _ga_7N8T3QXGY7=GS1.1.1658143217.16.1.1658144509.0; _oauth2_proxy_mesos=TWlsZXMgTWlhb3xtaWxlcy5taWFvQHNob3BlZS5jb218aHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EvQUl0YnZta2k0Qkx2UEF5VXZNa0ZSTkRaQjItRzNzOFN4Z2JvS29lZWF3enE9czk2LWM=|1659335386|y4ERLMfqRVK3DLR9kv7Us1RbTEU=; _ga_LJKSC5SW1J=GS1.1.1660707125.7.1.1660707837.0.0.0; _ga_6VXJESRVZJ=GS1.1.1660901298.68.1.1660901316.0.0.0; _gid=GA1.2.2026779342.1661141506; _ga_5H3PPWG2TZ=GS1.1.1661162128.187.1.1661162155.0.0.0; _ga_8SBPY6GX69=GS1.1.1661162179.126.1.1661162822.0.0.0; _ga_XWL9LM2WJ1=GS1.1.1661162179.125.1.1661162822.0.0.0; _ga_3KMTQ5WCQH=GS1.1.1661162179.53.1.1661162822.0.0.0; _ga=GA1.2.121277466.1625027658; _ga_N1FF831FCD=GS1.1.1661225893.339.0.1661225893.0.0.0; _ga_YDEY4KMXPF=GS1.1.1661225902.19.1.1661225904.0.0.0; DATA-SUITE-AUTH-userToken=eyJhbGciOiJIUzI1NiJ9.eyJEQVRBLVNVSVRFLUFVVEgtdXNlclRva2VuIjoie1widXNlcklkXCI6XCIyMjYwMTRcIixcInVzZXJOYW1lXCI6bnVsbCxcImRpc3BsYXlOYW1lXCI6XCJNaWxlcyBNaWFvICjoi5fpnZ7nuYEpXCIsXCJlbWFpbFwiOlwibWlsZXMubWlhb0BzaG9wZWUuY29tXCIsXCJoYWRvb3BBY2NvdW50XCI6XCJtaWxlcy5taWFvXCIsXCJqb2JDb2RlXCI6bnVsbCxcImpvYkRlc2NcIjpudWxsLFwibWFuYWdlck5hbWVcIjpcInFpaGFuZy5mZW5nXCIsXCJtYW5hZ2VyRW1haWxcIjpcInFpaGFuZy5mZW5nQHNob3BlZS5jb21cIixcImRlcHREZXNjMVwiOlwiQ2hpbmFcIixcImRlcHREZXNjMlwiOlwiU2hvcGVlXCIsXCJkZXB0RGVzYzNcIjpcIkRldmVsb3BtZW50IENlbnRlclwiLFwiZGVwdERlc2M0XCI6XCJTWlwiLFwiZGVwdERlc2M1XCI6XCJDdXN0b21lciBTZXJ2aWNlXCIsXCJkZXB0RGVzYzZcIjpcIlNvZnR3YXJlIERldmVsb3BtZW50XCIsXCJhdmF0YXJcIjpcImh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FBVFhBSnpSY1ZobGVqXzNLaXBmdVEwR3JxbnVReU1ib0dIRmFqSUJSQzd6PXM5Ni1jXCIsXCJkZWZhdWx0UHJvamVjdFwiOntcInByb2plY3RDb2RlXCI6XCJjaGF0Ym90X2RhdGFcIixcInByb2plY3ROYW1lXCI6XCJzaG9wZWVfY3NcIixcInByb2plY3RSb2xlXCI6XCJhZG1pbixkZXZcIn0sXCJwcm9qZWN0c1wiOlt7XCJwcm9qZWN0Q29kZVwiOlwiY2hhdGJvdF9kYXRhXCIsXCJwcm9qZWN0TmFtZVwiOlwic2hvcGVlX2NzXCIsXCJwcm9qZWN0Um9sZVwiOlwiYWRtaW4sZGV2XCJ9LHtcInByb2plY3RDb2RlXCI6XCJpbnN1cmFuY2VfY3NcIixcInByb2plY3ROYW1lXCI6XCJpbnN1cmFuY2VfY3NcIixcInByb2plY3RSb2xlXCI6XCJhZG1pbixkZXZcIn0se1wicHJvamVjdENvZGVcIjpcIm1wX2NzXCIsXCJwcm9qZWN0TmFtZVwiOlwiQ3VzdG9tZXIgU2VydmljZSBNYXJ0XCIsXCJwcm9qZWN0Um9sZVwiOlwiZGV2XCJ9LHtcInByb2plY3RDb2RlXCI6XCJwbGF5Z3JvdW5kX2NzXCIsXCJwcm9qZWN0TmFtZVwiOlwicGxheWdyb3VuZF9jc1wiLFwicHJvamVjdFJvbGVcIjpcImFkbWluLGRldlwifSx7XCJwcm9qZWN0Q29kZVwiOlwicmVjaGF0Ym90X2luaG91c2VcIixcInByb2plY3ROYW1lXCI6XCJzaG9wZWVfY3NfZG1zXCIsXCJwcm9qZWN0Um9sZVwiOlwiYWRtaW4sZGV2XCJ9XSxcImFjdGl2ZVwiOm51bGx9In0.LIB8_vKrxcJN_ovot68i65Frs09_JeS-EcdIElyjx5I");
        for (int i = 1; i <= 3; i++) {
            HttpResponse response = HttpUtil.httpGet("https://datasuite.shopee.io/datastudio/api/v1/metadata/search?schema=chatbot_data&keyWord=dws&pageNo=" + i + "&idcRegion=%24UNKNOWN", headers);
            JsonObject object = JsonUtil.toJsonObject(FileUtil.stream2string(response.getEntity().getContent()));
            JsonArray array = object.getAsJsonObject("data").getAsJsonArray("highlightedTables");
            for (JsonElement e : array) {
                System.out.println(e.getAsJsonObject().getAsJsonObject("tableEntity").get("name").getAsString());
            }
        }
    }
}