package cn.cnic.faird;

import cn.cnic.base.utils.*;
import cn.cnic.protocol.model.AbstractParser;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.Parser;
import cn.cnic.protocol.service.FairdService;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.vo.AskSqlVo;
import cn.cnic.protocol.vo.SelectReqVo;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import net.sf.json.JSONObject;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.util.ArrowUtils;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author yaxuan
 * @create 2023/9/26 15:00
 */
@Getter
@Setter
public class FairdServiceProducer extends NoOpFlightProducer implements AutoCloseable {

    private Location location = null;
    private BufferAllocator allocator= null;
    private FairdService fairdService;

    public FairdServiceProducer(String serviceClassName) {
        try {
            // 获取接口的实现类
            Class<?> clazz = Class.forName(serviceClassName);
            if (FairdService.class.isAssignableFrom(clazz)) {
                // 调用接口方法
                FairdService fairdServiceImpl = (FairdService) clazz.newInstance();
                this.fairdService = fairdServiceImpl;
            }
        } catch (ClassNotFoundException e) {
            System.out.println("未找到类：" + serviceClassName);
        } catch (InstantiationException | IllegalAccessException e) {
            System.out.println("无法实例化类：" + serviceClassName);
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        String dfId = descriptor.getPath().get(0);
        FlightEndpoint flightEndpoint = new FlightEndpoint(new Ticket(dfId.getBytes(StandardCharsets.UTF_8)), location);
        Dataset<Row> sparkDataFrame = FairdServer.dataFrameCache.get(dfId);
        Schema arrowSchema = ArrowUtils.toArrowSchema(sparkDataFrame.schema(), "Asia/Shanghai");
        long size = sparkDataFrame.count();
        return new FlightInfo(arrowSchema, descriptor, Collections.singletonList(flightEndpoint), -1, size);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        String dfId = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        Dataset<Row> sparkDataFrame = FairdServer.dataFrameCache.get(dfId);
        // to arrow: 暂时先转parquet
        List<ArrowRecordBatch> batches = new ArrayList<>();
        try {
            Path tempFilePath = Files.createTempFile("temp", ".parquet");
            if (Files.exists(tempFilePath)) {
                Files.delete(tempFilePath);
            }
            sparkDataFrame.write().format("parquet").option("path", tempFilePath.toString()).save();

            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
            //BufferAllocator allocator = new RootAllocator();
            DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, "file:" + tempFilePath.toString());
            org.apache.arrow.dataset.source.Dataset dataset = datasetFactory.finish();
            Scanner scanner = dataset.newScan(options);
            ArrowReader reader = scanner.scanBatches();
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    final VectorUnloader unloader = new VectorUnloader(root);
                    batches.add(unloader.getRecordBatch());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 输出
        Schema arrowSchema = ArrowUtils.toArrowSchema(sparkDataFrame.schema(), "Asia/Shanghai");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            VectorLoader loader = new VectorLoader(root);
            listener.start(root);
            for (ArrowRecordBatch arrowRecordBatch : batches) {
                loader.load(arrowRecordBatch);
                listener.putNext();
            }
            listener.completed();
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        switch (action.getType()) {
            case "META":
                metaAction(context, action, listener);
                return;
            case "MODEL":
                modelAction(context, action, listener);
                return;
            case "OPEN":
                openAction(context, action, listener);
                return;
            case "OPEN_WITH_PARSER":
                openWithParserAction(context, action, listener);
                return;
            case "OPEN_WITH_PARSER_NEW":
                openWithParserNewAction(context, action, listener);
                return;
            case "DOWNLOAD":
                downloadAction(context, action, listener);
                return;
            case "GET_SCHEMA":
                getSchema(context, action, listener);
                return;
            case "PRINT_SCHEMA":
                printSchema(context, action, listener);
                return;
            case "GET_LENGTH":
                getLength(context, action, listener);
                return;
            case "TO_JSON":
                toJson(context, action, listener);
                return;
            case "PRINT_DATA":
                printData(context, action, listener);
                return;
            case "GET_ITERATOR":
                getIterator(context, action, listener);
                return;
            case "LIMIT":
                limit(context, action, listener);
                return;
            case "SELECT":
                select(context, action, listener);
                return;
            case "ASK_SQL":
                askSql(context, action, listener);
                return;
            case "ASK_UNION":
                askUnion(context, action, listener);
                return;
            case "RUN_FLOW":
                runFlow(context, action, listener);
                return;
            case "GET_LOG":
                getLog(context, action, listener);
                return;
        }
    }

    private void metaAction(CallContext context, Action action, StreamListener<Result> listener) {
        MetaData metaData = fairdService.getMeta(UUIDUtils.getIdentifier(new String(action.getBody(), StandardCharsets.UTF_8)));
        Result metaRes = new Result(JSON.toJSONString(metaData).getBytes(StandardCharsets.UTF_8));
        listener.onNext(metaRes);
        listener.onCompleted();
    }

    private void modelAction(CallContext context, Action action, StreamListener<Result> listener) {
        String uri = new String(action.getBody(), StandardCharsets.UTF_8);
        List<DataFrame> dataFrameList = null;
        if (FairdServer.dataModelCache.get(uri) != null) {
            dataFrameList = FairdServer.dataModelCache.get(uri);
        } else {
            dataFrameList = fairdService.getData(UUIDUtils.getIdentifier(uri));
            FairdServer.dataModelCache.put(uri, dataFrameList);
        }
        for (DataFrame dataFrame : dataFrameList) {
            dataFrame.setRefUri(uri);
            FairdServer.dataFrameCache.put(dataFrame.getId(), dataFrame.getDataframeWithParser());
            JSONObject json = new JSONObject();
            json.put("id", dataFrame.getId());
            json.put("refUri", uri);
            Result modelRes = new Result(json.toString().getBytes(StandardCharsets.UTF_8));
            listener.onNext(modelRes);
        }
        listener.onCompleted();
    }

    private void openAction(CallContext context, Action action, StreamListener<Result> listener) {
        String req = new String(action.getBody(), StandardCharsets.UTF_8);
        String dataFrameId = req.split("_")[0];
        int index = Integer.parseInt(req.split("_")[1]);
        if (FairdServer.dataFrameCache.get(req) == null) {
            Dataset<Row> parent = FairdServer.dataFrameCache.get(dataFrameId);
            if (parent == null) {
                return;
            }
            Row row = parent.collectAsList().get(index);
            byte[] content = row.getAs("content");
            String parserClassName = row.getAs("parser");
            Parser parser = null;
            try {
                // 获取接口的实现类
                Class<?> clazz = Class.forName(parserClassName);
                if (Parser.class.isAssignableFrom(clazz)) {
                    parser = (Parser) clazz.newInstance();
                }
            } catch (ClassNotFoundException e) {
                System.out.println("未找到类：" + parserClassName);
            } catch (InstantiationException | IllegalAccessException e) {
                System.out.println("无法实例化类：" + parserClassName);
            }
            Dataset<Row> child = parser.toSparkDataFrame(content);
            FairdServer.dataFrameCache.put(req, child);
        }
    }

    private void openWithParserAction(CallContext context, Action action, StreamListener<Result> listener) {
        String req = new String(action.getBody(), StandardCharsets.UTF_8);
        String dataFrameId = req.split("_")[0];
        int index = Integer.parseInt(req.split("_")[1]);
        String parserId = req.split("_")[2];
        if (FairdServer.dataFrameCache.get(req) != null) {
            return;
        }
        Dataset<Row> parent = FairdServer.dataFrameCache.get(dataFrameId);
        if (parent == null) {
            return;
        }
        Row row = parent.collectAsList().get(index);
        byte[] content = row.getAs("content");
        // 创建临时文件
        Path jarTempFilePath;
        try {
            jarTempFilePath = Files.createTempFile("temp-jar-" + UUIDUtils.getUUID32(), ".jar");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        // 调用语义网parser download接口
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet("http://10.0.82.94:5000/download?_id=" + parserId);
        try {
            InputStream responseStream = httpClient.execute(httpGet).getEntity().getContent();
            FileOutputStream outputStream = new FileOutputStream(jarTempFilePath.toFile());
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = responseStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.close();
            responseStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 加载jar文件
        try {
            String className = "";
            JarFile jarFile = new JarFile(jarTempFilePath.toFile().getAbsoluteFile());
            Enumeration enu = jarFile.entries();
            while (enu.hasMoreElements()) {
                JarEntry jarEntry = (JarEntry) enu.nextElement();
                String name = jarEntry.getName();
                if (name.endsWith(".class") && !name.contains("/Parser")) {
                    className = name.replace('/', '.').replace(".class", "");
                    break;
                }
            }
            URLClassLoader classLoader = new URLClassLoader(new URL[]{jarTempFilePath.toFile().toURI().toURL()}, ClassLoader.getSystemClassLoader());
            Class<?> loadedClass = classLoader.loadClass(className);
            Parser parser = null;
            if (Parser.class.isAssignableFrom(loadedClass)) {
                parser = (Parser) loadedClass.newInstance();
            }
            Dataset<Row> child = parser.toSparkDataFrame(content, FairdServer.spark);
            FairdServer.dataFrameCache.put(req, child);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                Files.delete(jarTempFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void openWithParserNewAction(CallContext context, Action action, StreamListener<Result> listener) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(action.getBody());
            ObjectInputStream ois = new ObjectInputStream(bis);
            Map<String, Object> map = (Map<String, Object>) ois.readObject();
            ois.close();
            String dataframeId = map.get("dataframeId").toString();
            int index = Integer.parseInt(map.get("index").toString());
            String parserId = map.get("parserId").toString();
            Map<String, Object> properties = (Map<String, Object>)  map.get("properties");
            String openedDfId = dataframeId + "_" + index + "_" + parserId;
            if (FairdServer.dataFrameCache.get(openedDfId) != null) {
                return;
            }
            Dataset<Row> parent = FairdServer.dataFrameCache.get(dataframeId);
            if (parent == null) {
                return;
            }
            Row row = parent.collectAsList().get(index);
            byte[] content = row.getAs("content");
            Path jarFilePath = downloadJar(parserId);
            // 加载jar文件
            List<AbstractParser> parserList = loadJar(jarFilePath.toFile());
            AbstractParser parser = parserList.get(0);
            parser.setProperties(properties);
            Dataset<Row> child = parser.parse(content).getDataframe();
            FairdServer.dataFrameCache.put(openedDfId, child);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void downloadAction(CallContext context, Action action, StreamListener<Result> listener) {
//        String uri = new String(action.getBody(), StandardCharsets.UTF_8);
//        List<UrlElement> urlElements = fairdService.getUrl(UUIDUtils.getIdentifier(uri));
//        for (UrlElement urlElement : urlElements) {
//            JSONObject json = new JSONObject();
//            json.put("fileName", urlElement.getFileName());
//            json.put("httpUrl", urlElement.getHttpUrl());
//            json.put("size", urlElement.getSize());
//            Result res = new Result(json.toString().getBytes(StandardCharsets.UTF_8));
//            listener.onNext(res);
//        }
//        listener.onCompleted();
    }

    private void getSchema(CallContext context, Action action, StreamListener<Result> listener) {
        String id = new String(action.getBody(), StandardCharsets.UTF_8);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(df.schema());
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printSchema(CallContext context, Action action, StreamListener<Result> listener) {
        String id = new String(action.getBody(), StandardCharsets.UTF_8);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(df.schema().treeString());
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void toJson(CallContext context, Action action, StreamListener<Result> listener) {
        String id = new String(action.getBody(), StandardCharsets.UTF_8);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(df.drop("content").toJSON().collectAsList().toString());
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getLength(CallContext context, Action action, StreamListener<Result> listener) {
        String id = new String(action.getBody(), StandardCharsets.UTF_8);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        Result res = new Result(JSON.toJSONString(df.count()).getBytes(StandardCharsets.UTF_8));
        listener.onNext(res);
        listener.onCompleted();

    }

    private void printData(CallContext context, Action action, StreamListener<Result> listener) {
        String req = new String(action.getBody(), StandardCharsets.UTF_8);
        String[] args = req.split(",");
        String id = args[0];
        int numRows = Integer.parseInt(args[1]);
        int truncate = Integer.parseInt(args[2]);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            //oos.writeObject(df.showString(numRows, truncate, false));
            oos.writeObject(JsonUtils.formatJsonToTable(df.limit(numRows).toJSON().collectAsList().toString(), truncate));
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getIterator(CallContext context, Action action, StreamListener<Result> listener) {
        String id = new String(action.getBody(), StandardCharsets.UTF_8);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        JSONObject json = new JSONObject();
        json.put("dataframe", df.toJSON().collectAsList().toString());
        Result res = new Result(json.toString().getBytes(StandardCharsets.UTF_8));
        listener.onNext(res);
        listener.onCompleted();
    }

    private void limit(CallContext context, Action action, StreamListener<Result> listener) {
        String req = new String(action.getBody(), StandardCharsets.UTF_8);
        String id = req.split(",")[0];
        int n = Integer.parseInt(req.split(",")[1]);
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        if (df == null) {
            return;
        }
        String tempId = UUIDUtils.getUUID32();
        FairdServer.dataFrameCache.put(tempId, df.limit(n));
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(tempId);
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void select(CallContext context, Action action, StreamListener<Result> listener) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(action.getBody());
            ObjectInputStream ois = new ObjectInputStream(bis);
            SelectReqVo selectReqVo = (SelectReqVo) ois.readObject();
            ois.close();
            Dataset<Row> df = FairdServer.dataFrameCache.get(selectReqVo.getId());
            if (df == null) {
                return;
            }
            String tempId = UUIDUtils.getUUID32();
            FairdServer.dataFrameCache.put(tempId, df.select(selectReqVo.getField(), selectReqVo.getFields().toArray(new String[]{})));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(tempId);
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void askSql(CallContext context, Action action, StreamListener<Result> listener) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(action.getBody());
            ObjectInputStream ois = new ObjectInputStream(bis);
            AskSqlVo askSqlVo = (AskSqlVo) ois.readObject();
            ois.close();
            Dataset<Row> df = FairdServer.dataFrameCache.get(askSqlVo.getId());
            if (df == null) {
                return;
            }
            String tableName = SparkUtils.extractTableName(askSqlVo.getSqlQuery());
            df.createOrReplaceTempView(tableName);
            Dataset<Row> result = FairdServer.spark.sql(askSqlVo.getSqlQuery());
            String tempId = UUIDUtils.getUUID32();
            FairdServer.dataFrameCache.put(tempId, result);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(tempId);
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void askUnion(CallContext context, Action action, StreamListener<Result> listener) {
        String req = new String(action.getBody(), StandardCharsets.UTF_8);
        String id = req.split(",")[0];
        String unionId = req.split(",")[1];
        Dataset<Row> df = FairdServer.dataFrameCache.get(id);
        Dataset<Row> unionDf = FairdServer.dataFrameCache.get(unionId);
        if (df == null || unionDf == null) {
            return;
        }
        String tempId = UUIDUtils.getUUID32();
        FairdServer.dataFrameCache.put(tempId, df.join(unionDf));
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(tempId);
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runFlow(CallContext context, Action action, StreamListener<Result> listener) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(action.getBody());
            ObjectInputStream ois = new ObjectInputStream(bis);
            String flowJson = (String) ois.readObject();
            ois.close();
            // 请求piflow-server运行流水线
            String doPost = HttpUtils.doPost("http://10.0.90.210:8912/piflow-server/datacenter/flow/start", flowJson, null);
            String appId = JSONObject.fromObject(doPost).getJSONObject("group").getString("id");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(appId);
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getLog(CallContext context, Action action, StreamListener<Result> listener) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(action.getBody());
            ObjectInputStream ois = new ObjectInputStream(bis);
            String processId = (String) ois.readObject();
            ois.close();
            // 请求piflow-server
            List<Map<String, String>> logs = null;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(logs);
            oos.close();
            Result res = new Result(baos.toByteArray());
            listener.onNext(res);
            listener.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Path downloadJar(String parserId) {
        // 创建临时文件
        Path jarTempFilePath;
        try {
            jarTempFilePath = Files.createTempFile("temp-jar-" + UUIDUtils.getUUID32(), ".jar");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet("http://10.0.82.94:5000/download?_id=" + parserId);
        try {
            InputStream responseStream = httpClient.execute(httpGet).getEntity().getContent();
            FileOutputStream outputStream = new FileOutputStream(jarTempFilePath.toFile());
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = responseStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.close();
            responseStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jarTempFilePath;
    }

    private List<AbstractParser> loadJar(File jarFile) {
        List<AbstractParser> parserList = new ArrayList<>();
        try {
            String dependencyPath = "/Users/yaxuan/idea_workspace/faird-rpc/parser-configure/target/parser-configure-0.0.1-SNAPSHOT.jar";
            URL jarUrl = jarFile.toURI().toURL();
            URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl, new File(dependencyPath).toURI().toURL()});
            ZipInputStream zipInputStream = new ZipInputStream(jarUrl.openConnection().getInputStream());
            ZipEntry entry = null;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    if (entry.getName().endsWith(".class")) {
                        String entryName = entry.getName().substring(0, entry.getName().length() - 6).replaceAll("/", ".");
                        try {
                            Class<?> clazz = classLoader.loadClass(entryName);
                            Class<?> superclass = clazz.getSuperclass();
                            if (superclass.getName().equals("cn.cnic.protocol.model.AbstractParser")) {
                                AbstractParser parser = (AbstractParser) clazz.newInstance();
                                parserList.add(parser);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parserList;
    }

    @Override
    public void close() throws Exception {

    }
}