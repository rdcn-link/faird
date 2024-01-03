package cn.cnic.base.utils;

import cn.cnic.faird.FairdServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yaxuan
 * @create 2023/10/28 12:59
 */
public class ParserUtils {

    public static Dataset<Row> generateDfWithCommonParser(Dataset<Row> originDf) {
        // 文件类型
        if (isFileDataFrame(originDf)) {
            // 定义UDF函数，用于判断文件后缀并返回匹配的parser名称
            FairdServer.spark.udf().register("parserMatch", (UDF1<String, String>) fileName -> {
                String fileExtension = fileName.substring(fileName.lastIndexOf('.'));
                return getCommonParserMap().get(fileExtension);
            }, DataTypes.StringType);
            Dataset<Row> dfWithParser = originDf.withColumn("parser", functions.callUDF("parserMatch", originDf.col("fileName")));
            // 添加 openable 列，根据 parser 列的值确定
            dfWithParser = dfWithParser.withColumn("openable", functions.when(functions.col("parser").isNull(), false).otherwise(true).cast(DataTypes.BooleanType));
            return dfWithParser;
        }
        // 结构化类型
        Dataset<Row> dfWithParser = originDf.withColumn("parser", functions.lit(null).cast(DataTypes.StringType))
                .withColumn("openable", functions.lit(false).cast(DataTypes.BooleanType));
        return dfWithParser;
    }

    private static Map<String, String> getCommonParserMap() {
        Map<String, String> parserMap = new HashMap<>();
        // csv
        parserMap.put(".csv", "cn.cnic.protocol.parser.CsvParser");
        // excel
        parserMap.put(".xlsx", "cn.cnic.protocol.parser.ExcelParser");
        parserMap.put(".xls", "cn.cnic.protocol.parser.ExcelParser");
        parserMap.put(".xlsm", "cn.cnic.protocol.parser.ExcelParser");
        parserMap.put(".xlsb", "cn.cnic.protocol.parser.ExcelParser");
        // json
        parserMap.put(".json", "cn.cnic.protocol.parser.JsonParser");
        // image
        parserMap.put(".jpg", "cn.cnic.protocol.parser.ImageParser");
        parserMap.put(".jpeg", "cn.cnic.protocol.parser.ImageParser");
        parserMap.put(".png", "cn.cnic.protocol.parser.ImageParser");
        parserMap.put(".bmp", "cn.cnic.protocol.parser.ImageParser");
        parserMap.put(".tif", "cn.cnic.protocol.parser.ImageParser");
        return parserMap;
    }

    private static boolean isFileDataFrame(Dataset<Row> dataframe) {
        List<String> columns = Arrays.asList(dataframe.columns());
        if (!columns.contains("fileName") || !columns.contains("size") || !columns.contains("content")) {
            return false;
        }
        return true;
    }
}
