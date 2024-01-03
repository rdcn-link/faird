package cn.cnic.faird;

import cn.cnic.protocol.model.DataFrame;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yaxuan
 * @create 2023/9/26 14:45
 */
public class FairdServer {

    public static String serverName;
    public static FlightServer flightServer;
    public static SparkSession spark;
    public static Map<String, List<DataFrame>> dataModelCache;
    public static Map<String, Dataset<Row>> dataFrameCache;

    public FairdServer(FlightServer flightServer, String serverName) {
        this.flightServer = flightServer;
        this.serverName = serverName;
        SparkConf sparkConf = new SparkConf().setAppName("server spark").setMaster("local[*]").set("spark.driver.bindAddress", "0.0.0.0");
        this.spark =  SparkSession.builder().config(sparkConf).getOrCreate();
        dataModelCache = new HashMap<>();
        dataFrameCache = new HashMap<>();
    }

    public static FairdServer create(String serverName, String host, int port, String serviceClassName) {
        FairdServer server = FairdServer.builder(serverName, host, port, new FairdServiceProducer(serviceClassName));
        try {
            flightServer.start();
            System.out.println("Faird Server "+ serverName +" Start: Listening on port " + flightServer.getPort());
            flightServer.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return server;
    }

    private static FairdServer builder(String serverName, String host, int port, FairdServiceProducer producer) {
        Location location = Location.forGrpcInsecure(host, port);
        BufferAllocator allocator = new RootAllocator();
        producer.setLocation(location);
        producer.setAllocator(allocator);
        FlightServer server = FlightServer.builder(allocator, location, producer).build();
        return new FairdServer(server, serverName);
    }
}
