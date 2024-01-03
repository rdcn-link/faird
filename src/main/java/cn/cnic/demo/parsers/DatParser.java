package cn.cnic.demo.parsers;

import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yaxuan
 * @create 2023/10/30 15:34
 */
public class DatParser implements Parser {
    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return "自定义dat格式，该dat格式文件内容为十六进制，每行为一条数据，记录city_code, latitude, longitude, city_name信息。其中city_code，latitude，longitude为Integer类型，占4个字节；city_name为字符串类型。" +
                "进行python代码分析时，请使用df变量对pandas dataframe进行操作。例如：\n" +
                "1. 打印数据集 print(df)\n" +
                "2. 查询数据量 print(df.size)\n" +
                "3. 查询某一行数据 print(df.loc[0])\n" +
                "4. 查询某一列数据 print(df['colName'])\n";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        try {
            Path tempFile = Files.createTempFile("binary", ".dat");
            Files.write(tempFile, binary, StandardOpenOption.CREATE);
            Dataset<Row> sparkDf = FairdServer.spark.createDataFrame(readDataFromFile(tempFile.toFile().getAbsolutePath()), schema());
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
                DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
                DataTypes.createStructField("latitude", DataTypes.IntegerType, false),
                DataTypes.createStructField("longitude", DataTypes.IntegerType, false),
                DataTypes.createStructField("city_name", DataTypes.StringType, false),
        });
    }

    private static List<Row> readDataFromFile(String filePath) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(filePath);
        DataInputStream dataInputStream = new DataInputStream(fileInputStream);
        List<Row> data = new ArrayList<>();
        try {
            while (true) {
                // 读取cityCode字段
                byte[] cityCodeBytes = new byte[4];
                int bytesRead = dataInputStream.read(cityCodeBytes);
                if (bytesRead != 4) {
                    break;
                }
                int cityCode = byteArrayToInt(cityCodeBytes);
                // 读取latitude字段
                byte[] latitudeBytes = new byte[4];
                dataInputStream.read(latitudeBytes);
                int latitude = byteArrayToInt(latitudeBytes);
                // 读取longitude字段
                byte[] longitudeBytes = new byte[4];
                dataInputStream.read(longitudeBytes);
                int longitude = byteArrayToInt(longitudeBytes);
                // 读取cityName字段长度
                byte[] cityNameLenBytes = new byte[4];
                dataInputStream.read(cityNameLenBytes);
                int cityNameLen = byteArrayToInt(cityNameLenBytes);
                // 读取cityName字段
                byte[] cityNameBytes = new byte[cityNameLen];
                dataInputStream.read(cityNameBytes);
                String cityName = new String(cityNameBytes, StandardCharsets.UTF_8);
                // 创建Row对象并将数据添加到列表中
                Row row = RowFactory.create(cityCode, latitude, longitude, cityName);
                data.add(row);
            }
        } finally {
            dataInputStream.close();
            fileInputStream.close();
        }
        return data;
    }

    private static int byteArrayToInt(byte[] bytes) {
        return (bytes[0] & 0xFF) << 24 |
                (bytes[1] & 0xFF) << 16 |
                (bytes[2] & 0xFF) << 8 |
                (bytes[3] & 0xFF);
    }
}
