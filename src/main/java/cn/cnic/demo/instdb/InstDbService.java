package cn.cnic.demo.instdb;

import cn.cnic.base.utils.HttpClientUtils;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.parser.FileUrlParser;
import cn.cnic.protocol.parser.JsonParser;
import cn.cnic.protocol.service.FairdService;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.vo.UrlElement;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yaxuan
 * @create 2023/10/28 13:48
 */
public class InstDbService implements FairdService {

    final String InstdbUrl = "http://10.0.82.71:81/api";
    final String authCode = "zF4AxggDQZVICpEd5u";

    @Override
    public MetaData getMeta(String identifier) {
        String metaApiPath = InstdbUrl + "/getMetaData";
        String doGet = null;
        try {
            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("identifier", identifier);
            doGet = HttpClientUtils.doGetComCustomizeHeader(metaApiPath, jsonMap, null, null);
        } catch (Exception e) {
            return null;
        }
        if (StringUtils.isEmpty(doGet)) {
            return null;
        }
        net.sf.json.JSONObject dataObj = net.sf.json.JSONObject.fromObject(doGet).getJSONObject("data");
        Map<String, Class> classMap = new HashMap<>();
        classMap.put("title", MetaData.Title.class);
        classMap.put("identifier", MetaData.Identifier.class);
        classMap.put("dates", MetaData.Date.class);
        classMap.put("creators", MetaData.Creator.class);
        classMap.put("size", MetaData.Size.class);
        MetaData meta = (MetaData) net.sf.json.JSONObject.toBean(dataObj, MetaData.class, classMap);
        return meta;
    }

    @Override
    public List<DataFrame> getData(String identifier) {
        //先获取调取instdb接口需要的token
        String entryCode = getEntryCode();
        if (StringUtils.isBlank(entryCode)) {
            return null;
        }
        List<DataFrame> dataFrameList = new ArrayList<>();
        //token参数
        Map<String, String> headerParam = new HashMap<>();
        headerParam.put("token", entryCode);
        headerParam.put("version", "1.0");
        //文件处理
        List<UrlElement> resourceFileTree = getResourceFileTree(identifier, headerParam);
        if (null != resourceFileTree && resourceFileTree.size() > 0) {
            FileUrlParser parser = new FileUrlParser();
            Dataset<Row> sparkDf = parser.toSparkDataFrame(resourceFileTree);
            DataFrame dataFrame = new DataFrame(sparkDf);
            // 测试数据 todo
            if (identifier.equals("edc466013f4941e9972391a43f9019f1")) {
                dataFrame.setParser("regexp_replace(fileName, '^.*\\.dat$', '.dat') = '.dat'", "cn.cnic.demo.parsers.DatParser");
                dataFrame.setParser("regexp_replace(fileName, '^.*\\.fits$', '.fits') = '.fits'", "cn.cnic.demo.parsers.FitsParser");
                dataFrame.setParser("fileName = 'employee.txt'", "cn.cnic.demo.parsers.EmployeeTextParser");
                dataFrame.setParser("fileName = 'HNeph_2021-03-22_17-47-59_NCycle.txt'", "cn.cnic.demo.parsers.HNephTextParser");
            }
            dataFrameList.add(dataFrame);
        }
        //结构化处理
        List<DataFrame> structuredData = getStructuredData(identifier, headerParam);
        if (!structuredData.isEmpty() && structuredData.size() > 0) {
            dataFrameList.addAll(structuredData);
        }
        return dataFrameList;
    }

    //获取认证token
    public String getEntryCode() {
        String doGet = null;
        try {
            Map<String, String> headerParam = new HashMap<>();
            headerParam.put("secretKey", authCode);
            doGet = HttpClientUtils.doGetComCustomizeHeader(InstdbUrl + "/fair/entry", null, null, headerParam);

            if (StringUtils.isBlank(doGet)) {
                return doGet;
            }
            JSONObject jsonObject = JSONObject.parseObject(doGet);
            if (null != jsonObject) {
                List<Map<String, String>> serviceList = (List<Map<String, String>>) jsonObject.get("serviceList");
                if (null == serviceList || 0 == serviceList.size()) {
                    return null;
                }
                Map<String, Object> ticket = (Map<String, Object>) jsonObject.get("ticket");
                String token = (String) ticket.get("token");
                if (StringUtils.isNotBlank(token)) {
                    return token;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("获取接口认证token异常");
            return null;
        }
        return null;
    }

    //获取身份token
    public String getAuthCode(Map<String, String> headerParam) {
        String token = getEntryCode();
        if (StringUtils.isBlank(token)) {
            return null;
        }
        //instdb某个用户账号密码  后面这个地方看怎么联动 或者instdb单独再出接口
        Map<String, String> map = new HashMap<>();
        map.put("username", "admin@instdb.cn");
        map.put("password", "admin001");
        try {
            String resultData = HttpClientUtils.doGetComCustomizeHeader(InstdbUrl + "/open/getToken", map, null, headerParam);
            HashMap dataMap = JSONObject.parseObject(resultData, HashMap.class); //解析返回值
            int code = (int) dataMap.get("code");
            String accessToken = "";
            if (200 != code) {
                System.out.println("getUserToken请求错误，原因:" + dataMap.get("message").toString());
                return null;
            } else {
                Map data = (Map) dataMap.get("data");
                if (null != data && null != data.get("accessToken")) {
                    return data.get("accessToken").toString();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("获取用户身份token异常");
            return null;
        }
        return null;
    }

    //获取文件
    public List<UrlElement> getResourceFileTree(String identifier, Map<String, String> headerParam) {
        List<UrlElement> list = new ArrayList<>();
        //文件下载链接需要用户身份token  所有再获取下身份信息的token
        String authCode = getAuthCode(headerParam);
        if (StringUtils.isBlank(authCode)) {
            return list;
        }
        //去查询数据集下所有的文件
        String metaApiPath = InstdbUrl + "/open/getResourceFileTree";
        try {
            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("resourcesId", identifier);
            jsonMap.put("pageSize", "10000");
            jsonMap.put("pid", "983469079");
            jsonMap.put("pageOffset", "0");
            String getResourceFileTree = HttpClientUtils.doGetComCustomizeHeader(metaApiPath, jsonMap, null, headerParam);
            HashMap ResourceFileTree = JSONObject.parseObject(getResourceFileTree, HashMap.class); //解析返回值
            int codeResourceFileTree = (int) ResourceFileTree.get("code");
            if (200 != codeResourceFileTree) {
                System.out.println("获取数据集文件接口请求错误，原因:" + ResourceFileTree.get("message").toString());
                return list;
            } else {
                Map data = (Map) ResourceFileTree.get("data");
                JSONArray listData = (JSONArray) data.get("list");
                if (null != listData && listData.size() > 0) {
                    for (Object a : listData) {
                        JSONObject x = (JSONObject) JSON.toJSON(a);
                        //不是文件的过滤掉
                        if (!(boolean) x.get("isFile")) {
                            continue;
                        }
                        System.out.println(x.get("fileName"));
                        System.out.println(x.get("size"));
                        UrlElement urlElement = new UrlElement(x.get("fileName").toString(), x.getLong("size"), InstdbUrl + "/resources/resourcesDownloadFile?resourcesId=" + identifier + "&fileId=" + x.get("id") + "&token=" + authCode);
                        list.add(urlElement);
                    }
                    return list;
                }
                return list;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("数据集文件处理异常");
            return list;
        }
    }

    //获取结构化表格名称
    public List<String> getStructured(String identifier, Map<String, String> headerParam) {
        List<String> list = new ArrayList<>();
        //http://10.0.82.71:81/api/resources/getStructured?id=b78e577811e5445a985d0e6f4fa4a672
        String getStructuredPath = InstdbUrl + "/resources/getStructured";
        try {
            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("id", identifier);
            String getStructured = HttpClientUtils.doGetComCustomizeHeader(getStructuredPath, jsonMap, null, headerParam);
            HashMap StructuredMap = JSONObject.parseObject(getStructured, HashMap.class); //解析返回值
            int codeStructured = (int) StructuredMap.get("code");
            if (200 != codeStructured) {
                System.out.println("获取结构化表格名称接口请求错误，原因:" + StructuredMap.get("message").toString());
                return list;
            } else {
                JSONArray listData = (JSONArray) StructuredMap.get("data");
                if (null != listData && listData.size() > 0) {
                    for (Object a : listData) {
                        JSONObject x = (JSONObject) JSON.toJSON(a);
                        String tableName = (String) x.get("tableName");
                        list.add(tableName);
                    }
                    return list;
                }
                return list;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("获取结构化表格名称处理异常");
            return list;
        }
    }

    //获取结构化表格内容
    public List<DataFrame> getStructuredData(String identifier, Map<String, String> headerParam) {
        List<DataFrame> list = new ArrayList<>();
        //先拿到所有的结构化表格
        List<String> structured = getStructured(identifier, headerParam);
        if (structured.isEmpty()) {
            return list;
        }
        //依次循环去拿数据
        for (String tableName : structured) {
            // http://10.0.82.71:81/api/resources/getStructuredData?content=&id=b78e577811e5445a985d0e6f4fa4a672&name=structured_ydU0mW5v&pageOffset=0&pageSize=10
            String StructuredDataPath = InstdbUrl + "/resources/getStructuredData";
            try {
                Map<String, String> jsonMap = new HashMap<>();
                jsonMap.put("id", identifier);
                jsonMap.put("pageSize", "10000");
                jsonMap.put("name", tableName);
                jsonMap.put("pageOffset", "0");
                String getStructuredData = HttpClientUtils.doGetComCustomizeHeader(StructuredDataPath, jsonMap, null, headerParam);
                HashMap ResourceFileTree = JSONObject.parseObject(getStructuredData, HashMap.class); //解析返回值
                int codeResourceFileTree = (int) ResourceFileTree.get("code");
                if (200 != codeResourceFileTree) {
                    System.out.println("获取结构化表格内容接口请求错误，原因:" + ResourceFileTree.get("message").toString());
                    return list;
                } else {
                    Map data = (Map) ResourceFileTree.get("data");
                    JSONArray listData = (JSONArray) data.get("data");
                    if (null != listData && listData.size() > 0) {
                        String string = listData.toJSONString();
                        JsonParser jsonParser = new JsonParser();
                        Dataset<Row> sparkDf = jsonParser.toSparkDataFrame(string);
                        DataFrame dataFrame = new DataFrame(sparkDf);
                        list.add(dataFrame);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("获取结构化表格内容错误");
                return list;
            }
        }
        return list;
    }
}
