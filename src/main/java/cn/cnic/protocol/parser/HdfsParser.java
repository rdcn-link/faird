package cn.cnic.protocol.parser;

import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * @author yaxuan
 * @create 2023/10/28 10:34
 */
public class HdfsParser implements Parser {

    @Override
    public String name() {
        return "HdfsParser";
    }

    @Override
    public String description() {
        return "用于从HDFS中读取并解析文件数据，需提供hdfs url、hdfs path参数";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        return null;
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary, SparkSession sparkSession) {
        return null;
    }

    public Dataset<Row> toSparkDataFrame(String url, String path) {
        String fullUrl = url.contains(":")? url : url + ":9000";
        fullUrl = fullUrl.contains("hdfs://") ? url : "hdfs://" + url;
        Dataset<Row> df = FairdServer.spark.read().format("binaryFile").option("recursiveFileLookup", "true").load(fullUrl + path);
        df = df.withColumn("fileName", functions.expr("substring_index(input_file_name(), '/', -1)"))
                .withColumnRenamed("length", "size").drop("modificationTime");
        String[] columns = {"fileName", "size", "content"};
        df = df.selectExpr(columns);
        return df;
    }
}
