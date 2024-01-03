package cn.cnic.protocol.model;

import cn.cnic.base.utils.*;
import cn.cnic.base.vo.ActionMethod;
import cn.cnic.faird.FairdConnectionImpl;
import cn.cnic.protocol.vo.AskSqlVo;
import cn.cnic.protocol.vo.SelectReqVo;
import cn.cnic.protocol.vo.UnionAskMethod;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.sf.json.JSONObject;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yaxuan
 * @create 2023/10/28 10:26
 */
@Getter
@Setter
@NoArgsConstructor
public class DataFrame implements Serializable {

    private String id;
    private String refUri; // 关联的数据集uri
    private Dataset<Row> dataframe;
    private Dataset<Row> dataframeWithParser;

    public DataFrame(Dataset<Row> dataframe) {
        this.id = UUIDUtils.getUUID32();
        this.dataframe = dataframe;
        this.dataframeWithParser = ParserUtils.generateDfWithCommonParser(dataframe);
    }

    public void setParser(String filterExpression, String parserClassName) {
        Column filterColumn = functions.expr(filterExpression);
        Dataset<Row> filteredDf = dataframeWithParser.filter(filterColumn);
        Dataset<Row> updatedDF = filteredDf.withColumn("parser", functions.expr("'" + parserClassName + "'"))
                .withColumn("openable", functions.lit(true));
        Dataset<Row> res = updatedDF.unionAll(dataframeWithParser.except(filteredDf));
        this.dataframeWithParser = res;
    }

    /**
     * 将某一行打开为新的dataframe
     * @param index 行索引
     * @return
     */
    public DataFrame open(int index) {
        String req = id + "_" + index;
        FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
        Iterator<Result> openActionResult = connection.getFlightClient().doAction(new Action("OPEN", req.getBytes(StandardCharsets.UTF_8)));
        DataFrame openedDf = new DataFrame();
        openedDf.setId(req);
        openedDf.setRefUri(refUri);
        return openedDf;
    }

    public DataFrame open(int index, String parserId) {
        parserId = UUIDUtils.getIdentifier(parserId);
        String req = id + "_" + index + "_" + parserId;
        FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
        Iterator<Result> openActionResult = connection.getFlightClient().doAction(new Action("OPEN_WITH_PARSER", req.getBytes(StandardCharsets.UTF_8)));
        DataFrame openedDf = new DataFrame();
        openedDf.setId(req);
        openedDf.setRefUri(refUri);
        return openedDf;
    }

    public DataFrame open(int index, String parserId, Map<String, Object> properties) {
        try {
            Map<String, Object> reqVo = new HashMap<>();
            reqVo.put("dataframeId", id);
            reqVo.put("index", index);
            reqVo.put("parserId", parserId);
            reqVo.put("properties", properties);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(reqVo);
            oos.close();
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("OPEN_WITH_PARSER_NEW", bos.toByteArray()));
            DataFrame openedDf = new DataFrame();
            String openedDfId = id + "_" + index + "_" + parserId;
            openedDf.setId(openedDfId);
            openedDf.setRefUri(refUri);
            return openedDf;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * sql查询
     * @param sqlQuery
     * @return
     */
    public DataFrame ask(String sqlQuery) {
        long startTime = System.currentTimeMillis();
        DataFrame tempDf = new DataFrame();
        try {
            AskSqlVo askSqlVo = new AskSqlVo(id, SparkUtils.transSqlCode(sqlQuery));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(askSqlVo);
            oos.close();
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("ASK_SQL", bos.toByteArray()));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String tempId = (String) ois.readObject();
                tempDf.setId(tempId);
                ois.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.actionLogOk(ActionMethod.ASK, refUri, id, sqlQuery, startTime);
        return tempDf;
    }

    /**
     * 联合查询
     * @param dataframeId
     * @param method
     * @return
     */
    public DataFrame ask(String dataframeId, UnionAskMethod method) {
        DataFrame tempDf = new DataFrame();
        String req = id + "," + dataframeId;
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("ASK_UNION", req.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String tempId = (String) ois.readObject();
                tempDf.setId(tempId);
                ois.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tempDf;
    }

    /**
     * 获取StructType类型的schema
     * @return
     */
    public StructType getSchema() {
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("GET_SCHEMA", id.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                StructType structType = (StructType) ois.readObject();
                ois.close();
                return structType;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 输入打印格式的schema字符串
     * @return
     */
    public String printSchema() {
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("PRINT_SCHEMA", id.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String printSchema = (String) ois.readObject();
                ois.close();
                return printSchema;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取数据条数
     * @return
     */
    public long getLength() {
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("GET_LENGTH", id.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
                return Long.parseLong(resultStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取数据的json字符串
     * @return
     */
    public String toJsonStr() {
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("TO_JSON", id.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String toJsonStr = (String) ois.readObject();
                ois.close();
                return toJsonStr;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param numRows 显示的数据集行数;
     * @param truncate 截断的列的字符数;
     * @return
     */
    public String printData(int numRows, int truncate) {
        String req = id + "," + numRows + "," + truncate;
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("PRINT_DATA", req.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String printData = (String) ois.readObject();
                ois.close();
                return printData;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Iterator<Row> iterator() {
        SparkConf sparkConf = new SparkConf().setAppName("client").setMaster("local[*]").set("spark.driver.bindAddress", "0.0.0.0");
        SparkSession spark =  SparkSession.builder().config(sparkConf).getOrCreate();
        FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
        Iterator<Result> res = connection.getFlightClient().doAction(new Action("GET_ITERATOR", id.getBytes(StandardCharsets.UTF_8)));
        while (res.hasNext()) {
            Result result = res.next();
            String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
            JSONObject jsonObject = JSONObject.fromObject(resultStr);
            String dataframeJson = jsonObject.getString("dataframe");
            Dataset<Row> deserializedDf = spark.read().json(JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(Collections.singletonList(dataframeJson)));
            return deserializedDf.toLocalIterator();
        }
        return null;
    }

    public DataFrame limit(int n) {
        DataFrame tempDf = new DataFrame();
        String req = id + "," + n;
        try {
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("LIMIT", req.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String tempId = (String) ois.readObject();
                tempDf.setId(tempId);
                ois.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tempDf;
    }

    public DataFrame select(String field, String... fields) {
        DataFrame tempDf = new DataFrame();
        try {
            SelectReqVo selectReqVo = new SelectReqVo(id, field, Arrays.asList(fields));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(selectReqVo);
            oos.close();
            FairdConnectionImpl connection = ThreadLocalUtils.getConnect();
            Iterator<Result> res = connection.getFlightClient().doAction(new Action("SELECT", bos.toByteArray()));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String tempId = (String) ois.readObject();
                tempDf.setId(tempId);
                ois.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tempDf;
    }
}
