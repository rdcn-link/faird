package cn.cnic.protocol.parser;

import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author yaxuan
 * @create 2023/10/29 01:02
 */
public class ImageParser implements Parser {
    @Override
    public String name() {
        return "ImageParser";
    }

    @Override
    public String description() {
        return "用于解析图片类型文件数据，支持jpg/jpeg/png/tif等格式";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        try {
            Path tempFile = Files.createTempFile(UUIDUtils.getUUID32(), ".image");
            Files.write(tempFile, binary, StandardOpenOption.CREATE);
            Dataset<Row> sparkDf = FairdServer.spark.read().format("image").option("dropInvalid", true).load(tempFile.toFile().getAbsolutePath())
                    .select("image.origin", "image.height", "image.width", "image.nChannels", "image.mode", "image.data");
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

    private StructType schema() {
        return DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("origin", DataTypes.StringType, true),
                DataTypes.createStructField("height", DataTypes.IntegerType, true),
                DataTypes.createStructField("width", DataTypes.IntegerType, true),
                DataTypes.createStructField("nChannels", DataTypes.IntegerType, true),
                DataTypes.createStructField("mode", DataTypes.IntegerType, true),
                DataTypes.createStructField("data", DataTypes.BinaryType, true)
        });
    }
}
