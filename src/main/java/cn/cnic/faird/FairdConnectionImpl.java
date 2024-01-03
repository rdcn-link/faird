package cn.cnic.faird;

import cn.cnic.base.utils.LoggerUtil;
import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.base.vo.ActionMethod;
import cn.cnic.protocol.flow.*;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.vo.UrlElement;
import lombok.Getter;
import net.sf.json.JSONObject;
import org.apache.arrow.flight.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yaxuan
 * @create 2023/9/26 16:33
 */
@Getter
public class FairdConnectionImpl implements FairdConnection {

    private String ip;
    private int port;
    private String connectId;
    private String reqIp;
    private FlightClient flightClient;
    private SparkSession spark;

    public FairdConnectionImpl(String ip, int port, FlightClient flightClient) {
        this.ip = ip;
        this.port = port;
        this.connectId = UUIDUtils.getUUID32();
        this.flightClient = flightClient;
        SparkConf sparkConf = new SparkConf().setAppName("client spark").setMaster("local[*]").set("spark.driver.bindAddress", "0.0.0.0");
        this.spark = SparkSession.builder().config(sparkConf).getOrCreate();
    }

    public void setReqIp(String reqIp) {
        this.reqIp = reqIp;
    }

    @Override
    public void close() {
        try {
            this.flightClient.close();
            this.spark.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public MetaData meta(String uri) {
        long startTime = System.currentTimeMillis();
        MetaData metaData = null;
        Iterator<Result> metaActionResult = flightClient.doAction(new Action("META", uri.getBytes(StandardCharsets.UTF_8)));
        while (metaActionResult.hasNext()) {
            Result result = metaActionResult.next();
            String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
            JSONObject resultObj = JSONObject.fromObject(resultStr);
            Map<String, Class> classMap = new HashMap<>();
            classMap.put("title", MetaData.Title.class);
            classMap.put("identifier", MetaData.Identifier.class);
            classMap.put("dates", MetaData.Date.class);
            classMap.put("creators", MetaData.Creator.class);
            classMap.put("size", MetaData.Size.class);
            metaData = (MetaData) JSONObject.toBean(resultObj, MetaData.class, classMap);
        }
        LoggerUtil.actionLogOk(ActionMethod.META, uri, "", "", startTime);
        return metaData;
    }

    @Override
    public List<DataFrame> model(String uri) {
        long startTime = System.currentTimeMillis();
        List<DataFrame> dataFrameList = new ArrayList<>();
        try {
            Iterator<Result> modelActionResult = flightClient.doAction(new Action("MODEL", uri.getBytes(StandardCharsets.UTF_8)));
            while (modelActionResult.hasNext()) {
                Result result = modelActionResult.next();
                String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
                JSONObject jsonObject = JSONObject.fromObject(resultStr);
                String id = jsonObject.getString("id");
                String refUri = jsonObject.getString("refUri");
                DataFrame dataFrame = new DataFrame();
                dataFrame.setId(id);
                dataFrame.setRefUri(refUri);
                dataFrameList.add(dataFrame);
            }
        } catch (Exception e) {
            LoggerUtil.actionLogFail(ActionMethod.MODEL, uri, "", "", startTime);
            e.printStackTrace();
        }
        LoggerUtil.actionLogOk(ActionMethod.MODEL, uri, "", "", startTime);
        return dataFrameList;
    }

    @Override
    public Dataset<Row> getSparkDataframeById(String dataframeId) {
        Iterator<Result> res = flightClient.doAction(new Action("GET_ITERATOR", dataframeId.getBytes(StandardCharsets.UTF_8)));
        while (res.hasNext()) {
            Result result = res.next();
            String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
            JSONObject jsonObject = JSONObject.fromObject(resultStr);
            String dataframeJson = jsonObject.getString("dataframe");
            Dataset<Row> deserializedDf = spark.read().json(JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(Collections.singletonList(dataframeJson)));
            return deserializedDf;
        }
        return null;
    }

    @Override
    public String runFlow(Flow flow) {
        try {
            String flowJson = FlowUtils.flowBeanToFlowJson(flow);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(flowJson);
            oos.close();
            Iterator<Result> res = flightClient.doAction(new Action("RUN_FLOW", bos.toByteArray()));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = new ObjectInputStream(bais);
                String appId = (String) ois.readObject();
                ois.close();
                return appId;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String runFlow(String flowJson) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(flowJson);
            oos.close();
            Iterator<Result> res = flightClient.doAction(new Action("RUN_FLOW", bos.toByteArray()));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = new ObjectInputStream(bais);
                String appId = (String) ois.readObject();
                ois.close();
                return appId;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getLog(String processId) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(processId);
            oos.close();
            Iterator<Result> res = flightClient.doAction(new Action("GET_LOG", bos.toByteArray()));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = new ObjectInputStream(bais);
                String log = (String) ois.readObject();
                ois.close();
                return log;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<UrlElement> download(String uri) {
        List<UrlElement> urlElements = new ArrayList<>();
        try {
            Iterator<Result> res = flightClient.doAction(new Action("DOWNLOAD", uri.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
                JSONObject jsonObject = JSONObject.fromObject(resultStr);
                String fileName = jsonObject.getString("fileName");
                String httpUrl = jsonObject.getString("httpUrl");
                long size = Long.parseLong(jsonObject.getString("size"));
                UrlElement urlElement = new UrlElement(fileName, size ,httpUrl);
                urlElements.add(urlElement);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return urlElements;
    }
}
