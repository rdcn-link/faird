package cn.cnic.demo.parsers;

import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author yaxuan
 * @create 2023/10/30 16:05
 */
public class FitsParser implements Parser {
    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return "fits格式为天文学界常用的数据格式。进行python代码分析，请使用hdu变量，例如: \n" +
                "1. 查看数据集信息：hdu.info()\n" +
                "2. 查看头文件信息：print(hdu[0].header)\n" +
                "3. 查看数据信息：print(hdu[0].data)\n";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        try {
            Path tempFile = Files.createTempFile(UUIDUtils.getUUID32(), ".fits");
            Files.write(tempFile, binary, StandardOpenOption.CREATE);
            Dataset<Row> df = FairdServer.spark.read().format("com.astrolabsoftware.sparkfits")
                    .option("hdu", 1)
                    .load(tempFile.toFile().getAbsolutePath());
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
}
