package cn.cnic.protocol.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author yaxuan
 * @create 2023/12/14 14:02
 */
public class SparkUtil {

    public static SparkSession getSparkSession() {

        SparkConf sparkConf = new SparkConf().setAppName("server spark").setMaster("local[*]").set("spark.driver.bindAddress", "0.0.0.0");
        return SparkSession.builder().config(sparkConf).getOrCreate();
    }
}
