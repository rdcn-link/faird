package cn.cnic.demo.parsers;

import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.spark.SparkConf;
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
 * @create 2023/10/30 16:07
 */
public class EmployeeTextParser implements Parser {
    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return "自定义文本格式。进行python代码分析时，请使用df变量对pandas dataframe进行操作。例如：\n" +
                "1. 打印数据集 print(df)\n" +
                "2. 查询数据量 print(df.size)\n" +
                "3. 查询某一行数据 print(df.loc[0])\n" +
                "4. 查询某一列数据 print(df['colName'])\n";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        try {
            Path tempFile = Files.createTempFile("binary", ".txt");
            Files.write(tempFile, binary, StandardOpenOption.CREATE);
            Dataset<Row> sparkDf = FairdServer.spark.read().option("header", "false").option("inferSchema", "false").option("delimiter", "##")
                    .schema(schema()).csv(tempFile.toFile().getAbsolutePath());
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
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("gender", DataTypes.StringType, false),
                DataTypes.createStructField("salary", DataTypes.LongType, false),
                DataTypes.createStructField("expenses", DataTypes.LongType, false)
        });
    }
}
