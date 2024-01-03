package cn.cnic.faird;

import cn.cnic.base.utils.ThreadLocalUtils;
import cn.cnic.protocol.flow.Flow;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.vo.UrlElement;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author yaxuan
 * @create 2023/11/1 13:58
 */
public interface FairdConnection {

    static FairdConnection connect(String ip, int port, String reqIp) {
        Location location = Location.forGrpcInsecure(ip, port);
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        FairdConnectionImpl connection = new FairdConnectionImpl(ip, port, flightClient);
        connection.setReqIp(reqIp);
        ThreadLocalUtils.addConnection(connection);
        return connection;
    }

    void close();

    MetaData meta(String uri);

    List<DataFrame> model(String uri);

    Dataset<Row> getSparkDataframeById(String dataframeId);

    String runFlow(Flow flow);

    String runFlow(String flowJson);

    String getLog(String processId);

    List<UrlElement> download(String uri);
}
