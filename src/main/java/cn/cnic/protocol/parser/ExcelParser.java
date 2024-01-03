package cn.cnic.protocol.parser;

import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author yaxuan
 * @create 2023/10/29 01:01
 */
public class ExcelParser implements Parser {

    @Override
    public String name() {
        return "ExcelParser";
    }

    @Override
    public String description() {
        return "用于解析标准excel类型文件，包括xls/xlsx/xlsm/xlsb等格式";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        try {
            Path tempFile = Files.createTempFile(UUIDUtils.getUUID32(), ".xlsx");
            Files.write(tempFile, binary, StandardOpenOption.CREATE);
            Dataset<Row> sparkDf = FairdServer.spark.read().format("com.crealytics.spark.excel").option("header", true).load(tempFile.toFile().getAbsolutePath());
            return sparkDf;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary, SparkSession sparkSession) {
        return null;
    }
}
