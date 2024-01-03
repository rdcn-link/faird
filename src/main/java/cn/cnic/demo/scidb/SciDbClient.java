package cn.cnic.demo.scidb;

import cn.cnic.faird.FairdConnection;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;

import java.util.List;

/**
 * @author yaxuan
 * @create 2023/11/3 18:07
 */
public class SciDbClient {

    public static void main(String[] args) {

        FairdConnection connect = FairdConnection.connect("10.0.87.113", 3101, "10.0.90.210");
        // meta
        String uri = "fair://10.0.87.113/10.11922/sciencedb.15";
        MetaData metaData = connect.meta(uri);
        // model
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame dataFrame = dataFrameList.get(0);
        String s = dataFrame.printSchema();
        String json = dataFrame.toJsonStr();
        DataFrame df = dataFrame.open(0);
        String s1 = df.printSchema();
        System.out.println("*******");
    }
}
