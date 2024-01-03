package cn.cnic.protocol.parser;

import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

/**
 * @author yaxuan
 * @create 2023/10/29 01:00
 */
public class JsonParser implements Parser {
    @Override
    public String name() {
        return "JsonParser";
    }

    @Override
    public String description() {
        return "用于解析标准json格式文件";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        try {
            Path tempFile = Files.createTempFile(UUIDUtils.getUUID32(), ".json");
            Files.write(tempFile, binary, StandardOpenOption.CREATE);
            Dataset<Row> df = FairdServer.spark.read().format("json").option("multiLine", true).option("mode", "PERMISSIVE").load(tempFile.toFile().getAbsolutePath());
            return df;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary, SparkSession sparkSession) {
        return null;
    }

    public Dataset<Row> toSparkDataFrame(String jsonStr) {
        Dataset<Row> df = FairdServer.spark.read().json(JavaSparkContext.fromSparkContext(FairdServer.spark.sparkContext()).parallelize(Collections.singletonList(jsonStr)));
        return df;
    }
}
