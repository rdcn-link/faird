package cn.cnic.fairdrpc;

import cn.cnic.demo.scidb.SciDbService;
import cn.cnic.faird.FairdConnection;
import cn.cnic.protocol.flow.*;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.model.Parser;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * @author yaxuan
 * @create 2023/10/28 13:06
 */
public class Test {

    @org.junit.jupiter.api.Test
    void test() {
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
        // meta
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        MetaData metaData = connect.meta(uri);

        List<DataFrame> dataFrameList = connect.model(uri);
        // fileDataFrame对应文件数据的表格
        DataFrame df = dataFrameList.get(0);
        String data = df.printData(10, 20);
        DataFrame openedDf = df.open(0);

        Dataset<Row> sparkDf = null;



        String dataFrameId = openedDf.getId(); // 进一步将第0个文件打开为一个新的csvDf

        Thread child = new Thread(new Runnable() {
            @Override
            public void run() {
                FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "10.0.90.210");
                DataFrame temp = new DataFrame();
                temp.setId(dataFrameId);
                System.out.println(temp.getId());
                StructType schema = temp.getSchema();
                System.out.println(temp.printSchema());
            }
        });

        child.start();

        try {
            child.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @org.junit.jupiter.api.Test
    void test1() {
        SciDbService service = new SciDbService();
        MetaData metaData = service.getMeta("10.57760/sciencedb.IGA.00723");
        List<DataFrame> data = service.getData("10.57760/sciencedb.IGA.00723");
    }

    @org.junit.jupiter.api.Test
    void testFlow() throws Exception {
        FairdConnection connect = FairdConnection.connect("localhost", 3101, "10.0.90.210");
//        String uri = "fair://10.0.82.71/9cb9d0d870d648ea8022db5f1ba4ce4c";
//        List<DataFrame> dataFrameList = connect.model(uri);
//        DataFrame df = dataFrameList.get(0);

        BigFlowConnectionImpl bigFlowConnection = BigFlowConnection.Connect("http://10.0.90.210:6911", "admin", "ptKpbrwjpjCvtIwyPpao0Q==");
        Stop csvStringParser = bigFlowConnection.getLocalModel("CsvStringParser");
        Map<String,String> properties = new HashMap<>();
        properties.put("schema","name,province,phoneNumber,email");
        properties.put("delimiter",",");
        properties.put("string", "yaxuan,shandong,19862775895A,yaxuanZH@cnic.cn");
        csvStringParser.setProperty(properties);

        Flow flow = new Flow("test");
        flow.addStop(csvStringParser);
        connect.runFlow(flow);




    }

    @org.junit.jupiter.api.Test
    void testParser() throws IOException {
        //String jarFilePath = "/Users/yaxuan/Desktop/semantic/csvParser.jar";
        String jarFilePath = "/Users/yaxuan/idea_workspace/faird-parser/target/parser.jar";
        JarFile jarFile = new JarFile(jarFilePath);
        Enumeration enu = jarFile.entries();
        while (enu.hasMoreElements()) {
            JarEntry jarEntry = (JarEntry) enu.nextElement();
            String name = jarEntry.getName();
            if (name.endsWith(".class") && !name.contains("/Parser")) {
                String className = name.replace('/', '.').replace(".class", "");
                System.out.println("Class name: " + className);
            }
        }
        try {
            URLClassLoader classLoader = new URLClassLoader(new URL[]{new File(jarFilePath).toURI().toURL()}, ClassLoader.getSystemClassLoader());
            String className2 = "cn.cnic.protocol.model.CsvParser";
            Class<?> loadedClass = classLoader.loadClass(className2);
            Parser parser = null;
            if (Parser.class.isAssignableFrom(loadedClass)) {
                parser = (Parser) loadedClass.newInstance();
            }
            System.out.println(parser.name());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @org.junit.jupiter.api.Test
    void test0() {
        SparkConf conf = new SparkConf()
                .setAppName("Read FTP File to RDD Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // FTP 服务器信息
        String host = "10.0.82.71";
        int port = 2121;
        String username = "4hjqba";
        String password = "Q5DkqC";

        // FTP 文件路径
        String filePath = "ftp://" + username + ":" + password + "@" + host + ":" + port + "/ACSM-AE33.xlsx";

        // 读取文件为 RDD
        JavaRDD<String> rdd = sc.textFile(filePath);
        System.out.println(rdd.first());

        sc.stop();
    }

    @org.junit.jupiter.api.Test
    void test2() throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("client spark").setMaster("local[*]").set("spark.driver.bindAddress", "0.0.0.0");
        SparkSession spark =  SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> sparkDataFrame = spark.read().csv("/Users/yaxuan/idea_workspace/faird-rpc/src/test/testData/2019年中国榆林市沟道信息.csv");

        Path tempFilePath = Files.createTempFile("temp", ".parquet");
        if (Files.exists(tempFilePath)) {
            Files.delete(tempFilePath);
        }
        sparkDataFrame.write().format("parquet").option("path", tempFilePath.toString()).save();

        //String uri = "file:/opt/example.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(),
                        FileFormat.PARQUET, "file:" + tempFilePath.toString());
                org.apache.arrow.dataset.source.Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            List<ArrowRecordBatch> batches = new ArrayList<>();
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    final VectorUnloader unloader = new VectorUnloader(root);
                    batches.add(unloader.getRecordBatch());
                }
            }
            long size = batches.get(0).getLength();
            System.out.println(size);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
