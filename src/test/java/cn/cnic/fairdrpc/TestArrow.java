package cn.cnic.fairdrpc;

import cn.cnic.base.utils.SparkUtils;
import cn.cnic.protocol.vo.AskSqlVo;
import net.sf.json.JSONObject;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yaxuan
 * @create 2023/12/25 14:58
 */
public class TestArrow {

    @Test
    void getMeta() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数uri，请求方法META Action，返回结果resultStr
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        Iterator<Result> metaActionResult = flightClient.doAction(new Action("META", uri.getBytes(StandardCharsets.UTF_8)));
        while (metaActionResult.hasNext()) {
            Result result = metaActionResult.next();
            String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
            System.out.println(resultStr);
        }
    }

    @Test
    void getDataFrameList() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数uri，请求方法MODEL Action，返回结果dataFrameList
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<String> dataFrameList = new ArrayList<>();
        try {
            Iterator<Result> modelActionResult = flightClient.doAction(new Action("MODEL", uri.getBytes(StandardCharsets.UTF_8)));
            while (modelActionResult.hasNext()) {
                Result result = modelActionResult.next();
                String resultStr = new String(result.getBody(), StandardCharsets.UTF_8);
                JSONObject jsonObject = JSONObject.fromObject(resultStr);
                String id = jsonObject.getString("id");
                dataFrameList.add(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(dataFrameList);
    }

    @Test
    void getSchema() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数id，请求方法getSchema，返回结果schema
        String id = "61eb6386562d4725ae8a550343d10b93_0";
        Schema schema = flightClient.getSchema(FlightDescriptor.path(id)).getSchema();
        System.out.println(schema);
    }

    @Test
    void getSize() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数id，请求方法getInfo，返回结果size
        String id = "61eb6386562d4725ae8a550343d10b93_0";
        long size = flightClient.getInfo(FlightDescriptor.path(id)).getRecords();
        System.out.println(size);
    }

    @Test
    void getStream() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数id，请求方法getStream，返回结果flightStream数据流
        String id = "61eb6386562d4725ae8a550343d10b93_0";
        FlightStream flightStream =  flightClient.getStream(new Ticket(id.getBytes(StandardCharsets.UTF_8)));
        int batch = 0;
        try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
            while (flightStream.next()) {
                String data = vectorSchemaRootReceived.contentToTSVString();
                batch++;
                System.out.println("Received batch #" + batch + ", Data: " + data);
            }
        }
    }

    @Test
    void limit() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数（id,n），请求方法LIMIT action，返回结果tempId
        String id = "61eb6386562d4725ae8a550343d10b93";
        int n = 3;
        String req = id + "," + n;
        try {
            Iterator<Result> res = flightClient.doAction(new Action("LIMIT", req.getBytes(StandardCharsets.UTF_8)));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String tempId = (String) ois.readObject();
                ois.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void sql() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数AskSqlVo，请求方法ASK_SQL action，返回结果tempId
        String id = "61eb6386562d4725ae8a550343d10b93";
        String sqlQuery = "select * from temp";
        try {
            AskSqlVo askSqlVo = new AskSqlVo(id, SparkUtils.transSqlCode(sqlQuery));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(askSqlVo);
            oos.close();
            Iterator<Result> res = flightClient.doAction(new Action("ASK_SQL", bos.toByteArray()));
            while (res.hasNext()) {
                Result result = res.next();
                ByteArrayInputStream bais = new ByteArrayInputStream(result.getBody());
                ObjectInputStream ois = null;
                ois = new ObjectInputStream(bais);
                String tempId = (String) ois.readObject();
                ois.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void open() {
        // 建立连接
        Location location = Location.forGrpcInsecure("10.0.82.71", 3101);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        // 请求参数id_index，请求方法getStream，返回结果data流
        String id = "61eb6386562d4725ae8a550343d10b93";
        int index = 0;
        String req = id + "_" + index;
        Iterator<Result> openActionResult = flightClient.doAction(new Action("OPEN", req.getBytes(StandardCharsets.UTF_8)));
        String tempId = req;
    }
}
