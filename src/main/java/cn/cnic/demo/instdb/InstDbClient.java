package cn.cnic.demo.instdb;

import cn.cnic.base.utils.HttpClientUtils;
import cn.cnic.faird.FairdConnection;
import cn.cnic.protocol.flow.*;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.vo.UrlElement;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yaxuan
 * @create 2023/10/28 14:41
 */
public class InstDbClient {

    public static void main(String[] args) {
        //testDownload();
        //testCsvParser();
        //testXlsxParser();
        //testJsonParser();
        //testFlow0();
        //testFlow1();
        //testFlow2();
        testRunFlowJson();
    }

    public static void testDownload() {
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<UrlElement> urlElements = connect.download(uri);
        System.out.println("aaaa");
    }

    public static void testCsvParser() {
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3102, "10.0.90.210");
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        //String uri = "fair://10.0.87.113/10.11922/sciencedb.876";
        MetaData meta = connect.meta(uri);
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame df = dataFrameList.get(0);
        String s0 = df.printData(10, 20);
        String s1 = dataFrameList.get(1).printData(10, 20);
        String s2 = dataFrameList.get(2).printData(10, 20);
        String s3 = dataFrameList.get(3).printData(10, 20);
        String s4 = dataFrameList.get(4).printData(10, 20);
        DataFrame csvDf = df.open(0, getParserUri("csv"));
        String schema = csvDf.printSchema();
        String data = csvDf.printData(10, 20);
        connect.close();
    }

    public static void testXlsxParser() {
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame df = dataFrameList.get(0);
        DataFrame csvDf = df.open(1, getParserUri("xlsx"));
        String schema = csvDf.printSchema();
        String data = csvDf.printData(10, 20);
        connect.close();
    }

    public static void testJsonParser() {
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame df = dataFrameList.get(0);
        DataFrame csvDf = df.open(4, getParserUri("json"));
        String schema = csvDf.printSchema();
        String data = csvDf.printData(10, 20);
        connect.close();
    }

    public static void testFlow0() {
        Flow flow = new Flow("test0");

        /**
         * 使用faird dataframe作为flow数据源
         */
        FairdConnection connect = FairdConnection.connect("10.0.87.113", 3101, "");
        String uri = "fair://10.0.87.113/10.11922/sciencedb.876";
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame dataFrame = dataFrameList.get(0);

        flow.addStop("scienceDB", dataFrame.getId());
        String processId = connect.runFlow(flow);
        connect.close();
    }

    public static void testRunFlowJson() {
        String flowJson = "{\n" +
                "  \"flow\" : {\n" +
                "    \"executorNumber\" : \"1\",\n" +
                "    \"driverMemory\" : \"1g\",\n" +
                "    \"executorMemory\" : \"1g\",\n" +
                "    \"executorCores\" : \"1\",\n" +
                "    \"paths\" : [ {\n" +
                "      \"inport\" : \"\",\n" +
                "      \"from\" : \"ReadFaird\",\n" +
                "      \"to\" : \"AddUUIDStop\",\n" +
                "      \"outport\" : \"\"\n" +
                "    }, {\n" +
                "      \"inport\" : \"\",\n" +
                "      \"from\" : \"AddUUIDStop\",\n" +
                "      \"to\" : \"ProvinceClean\",\n" +
                "      \"outport\" : \"\"\n" +
                "    }, {\n" +
                "      \"inport\" : \"\",\n" +
                "      \"from\" : \"Join\",\n" +
                "      \"to\" : \"ShowData\",\n" +
                "      \"outport\" : \"\"\n" +
                "    }, {\n" +
                "      \"inport\" : \"Right\",\n" +
                "      \"from\" : \"ReadFaird-1704032732234-1704032732234\",\n" +
                "      \"to\" : \"Join\",\n" +
                "      \"outport\" : \"\"\n" +
                "    }, {\n" +
                "      \"inport\" : \"Left\",\n" +
                "      \"from\" : \"ProvinceClean\",\n" +
                "      \"to\" : \"Join\",\n" +
                "      \"outport\" : \"\"\n" +
                "    } ],\n" +
                "    \"name\" : \"0\",\n" +
                "    \"stops\" : [ {\n" +
                "      \"customizedProperties\" : { },\n" +
                "      \"dataCenter\" : \"\",\n" +
                "      \"name\" : \"ShowData\",\n" +
                "      \"uuid\" : \"6eef362dab4e449e8cc355fa5bf7fa5e\",\n" +
                "      \"bundle\" : \"cn.piflow.bundle.external.ShowData\",\n" +
                "      \"properties\" : {\n" +
                "        \"showNumber\" : \"10\"\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"customizedProperties\" : { },\n" +
                "      \"dataCenter\" : \"\",\n" +
                "      \"name\" : \"ReadFaird\",\n" +
                "      \"uuid\" : \"7247671ac15e476bb8ba7c4876ba7f68\",\n" +
                "      \"bundle\" : \"cn.piflow.bundle.faird.ReadFaird\",\n" +
                "      \"properties\" : {\n" +
                "        \"serviceIp\" : \"10.0.82.71\",\n" +
                "        \"dataframeId\" : \"eb19fa6e9633451fb1e4c9e707f9241f\",\n" +
                "        \"servicePort\" : \"3101\"\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"customizedProperties\" : { },\n" +
                "      \"dataCenter\" : \"\",\n" +
                "      \"name\" : \"AddUUIDStop\",\n" +
                "      \"uuid\" : \"d047c23d10f8416da3d60d6c20719a1e\",\n" +
                "      \"bundle\" : \"cn.piflow.bundle.common.AddUUIDStop\",\n" +
                "      \"properties\" : {\n" +
                "        \"column\" : \"uuid\"\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"customizedProperties\" : { },\n" +
                "      \"dataCenter\" : \"\",\n" +
                "      \"name\" : \"Join\",\n" +
                "      \"uuid\" : \"c30013bd819f4ca28c6d4310fac3820a\",\n" +
                "      \"bundle\" : \"cn.piflow.bundle.common.Join\",\n" +
                "      \"properties\" : {\n" +
                "        \"joinMode\" : \"inner\",\n" +
                "        \"correlationColumn\" : \"_id\"\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"customizedProperties\" : { },\n" +
                "      \"dataCenter\" : \"\",\n" +
                "      \"name\" : \"ProvinceClean\",\n" +
                "      \"uuid\" : \"8cb90702d06f42378477942b626685d1\",\n" +
                "      \"bundle\" : \"cn.piflow.bundle.clean.ProvinceClean\",\n" +
                "      \"properties\" : {\n" +
                "        \"columnName\" : \"province\"\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"customizedProperties\" : { },\n" +
                "      \"dataCenter\" : \"\",\n" +
                "      \"name\" : \"ReadFaird-1704032732234-1704032732234\",\n" +
                "      \"uuid\" : \"3ed1b0294aa54f87858d92bf3370f20a\",\n" +
                "      \"bundle\" : \"cn.piflow.bundle.faird.ReadFaird\",\n" +
                "      \"properties\" : {\n" +
                "        \"serviceIp\" : \"10.0.82.71\",\n" +
                "        \"dataframeId\" : \"76a814a707544c868cfeba211e9fca6a\",\n" +
                "        \"servicePort\" : \"3101\"\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"uuid\" : \"6de19e3517624a8d8be274e11ecb197c\"\n" +
                "  }\n" +
                "}";
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        String appId = connect.runFlow(flowJson);
        System.out.println(appId);

    }

    /**
     * 测试单一faird数据源流水线运行
     */
    public static void testFlow1() {
        Flow flow = new Flow("test1");

        /**
         * 使用faird dataframe作为flow数据源
         */
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame dataFrame = dataFrameList.get(3);

        /**
         * 查找并使用bigflow标准算子
         */
        BigFlowConnectionImpl bigFlowConnection = BigFlowConnection.Connect("http://10.0.90.210:6911", "admin", "ptKpbrwjpjCvtIwyPpao0Q==");
        // 算子1: AddUUIDStop [增加uuid列]
        Stop addUUIDStop = bigFlowConnection.getLocalModel("AddUUIDStop");
        addUUIDStop.setProperty(Collections.singletonMap("column", "uuid"));
        // 算子2: IdentityNumberClean [身份证号数据清洗]
        Stop identityNumberClean = bigFlowConnection.getLocalModel("IdentityNumberClean");
        identityNumberClean.setProperty(Collections.singletonMap("columnName", "identityNumber"));
        // 算子3: ProvinceClean [省份信息数据清洗]
        Stop provinceClean = bigFlowConnection.getLocalModel("ProvinceClean");
        provinceClean.setProperty(Collections.singletonMap("columnName", "province"));
        // 算子4: EmailClean [邮箱信息数据清洗]
        Stop emailClean = bigFlowConnection.getLocalModel("EmailClean");
        emailClean.setProperty(Collections.singletonMap("columnName", "email"));
        // 算子5: showData [展示结果]
        Stop showData = bigFlowConnection.getLocalModel("ShowData");
        showData.setProperty(Collections.singletonMap("showNumber", "10"));

        /**
         * 为flow配置算子和路径
         */
        // 配置算子
        flow.addStop("青藏高原亚洲高山区12米高分三号影像数据集-员工信息表", dataFrame.getId())
                .addStop(addUUIDStop)
                .addStop(identityNumberClean)
                .addStop(provinceClean)
                .addStop(emailClean)
                .addStop(showData);
        // 配置路径
        Path path1 = new Path("青藏高原亚洲高山区12米高分三号影像数据集-员工信息表", addUUIDStop.getName(), "", "");
        Path path2 = new Path(addUUIDStop.getName(), identityNumberClean.getName(), "", "");
        Path path3 = new Path(identityNumberClean.getName(), provinceClean.getName(), "", "");
        Path path4 = new Path(provinceClean.getName(), emailClean.getName(), "", "");
        Path path5 = new Path(emailClean.getName(), showData.getName(), "", "");
        flow.addPath(path1).addPath(path2).addPath(path3).addPath(path4).addPath(path5);

        /**
         * 运行flow
         */
        String processId = connect.runFlow(flow);
        connect.close();
    }

    /**
     * 测试多faird数据源流水线运行
     */
    public static void testFlow2() {
        Flow flow = new Flow("test2");
        /**
         * 使用faird dataframe作为flow数据源
         */
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        // dataframe1
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame dataFrame1 = dataFrameList.get(1);
        // dataframe2
        String uri2 = "fair://10.0.82.71/edc466013f4941e9972391a43f9019f1";
        List<DataFrame> dataFrameList2 = connect.model(uri2);
        DataFrame dataFrame2 = dataFrameList2.get(2);

        /**
         * 查找并使用bigflow标准算子
         */
        BigFlowConnectionImpl bigFlowConnection = BigFlowConnection.Connect("http://10.0.90.210:6911", "admin", "ptKpbrwjpjCvtIwyPpao0Q==");
        // 算子1: SelectField [选择某几列]
        Stop selectField = bigFlowConnection.getLocalModel("SelectField");
        selectField.setProperty(Collections.singletonMap("columnNames", "ip,type,url"));
        // 算子2: Filter [根据某列条件进行过滤]
        Stop filter = bigFlowConnection.getLocalModel("Filter");
        filter.setProperty(Collections.singletonMap("condition", "status_code=='200'"));
        // 算子3: Join [两个数据源join]
        Stop join = bigFlowConnection.getLocalModel("Join");
        Map<String,String> properties = new HashMap<>();
        properties.put("correlationColumn","ip");
        properties.put("joinMode","inner");
        join.setProperty(properties);
        // 算子4: showData [展示结果]
        Stop showData = bigFlowConnection.getLocalModel("ShowData");
        showData.setProperty(Collections.singletonMap("showNumber", "10"));

        /**
         * 为flow配置算子和路径
         */
        // 配置算子
        flow.addStop("青藏高原亚洲高山区12米高分三号影像数据集-服务器信息表", dataFrame1.getId())
                .addStop("四川亚米级泥石流灾害航空遥感影像数据集-访问日志表", dataFrame2.getId())
                .addStop(selectField)
                .addStop(filter)
                .addStop(join)
                .addStop(showData);
        // 配置路径
        Path path1 = new Path("青藏高原亚洲高山区12米高分三号影像数据集-服务器信息表", selectField.getName(), "", "");
        Path path2 = new Path("四川亚米级泥石流灾害航空遥感影像数据集-访问日志表", filter.getName(), "", "");
        Path path3 = new Path(selectField.getName(), join.getName(), "Left", "");
        Path path4 = new Path(filter.getName(), join.getName(), "Right", "");
        Path path5 = new Path(join.getName(), showData.getName(), "", "");
        flow.addPath(path1).addPath(path2).addPath(path3).addPath(path4).addPath(path5);

        /**
         * 运行flow
         */
        String processId = connect.runFlow(flow);
        connect.close();

    }

    private static String getParserUri(String format) {
        try {
            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("Format", format);
            String doGet = HttpClientUtils.doGetComCustomizeHeader("http://10.0.82.94:5000/list_parsers", jsonMap, null, null);
            JSONArray jsonArray = JSONArray.fromObject(doGet);
            if (jsonArray.size() == 0) {
                return null;
            }
            JSONObject firstObject = jsonArray.getJSONObject(0);
            String parserId = firstObject.getString("ID");
            return "fair://10.0.82.94/" + parserId;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
