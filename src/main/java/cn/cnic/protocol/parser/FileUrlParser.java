package cn.cnic.protocol.parser;

import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import cn.cnic.protocol.vo.UrlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yaxuan
 * @create 2023/10/28 11:59
 */
public class FileUrlParser implements Parser {

    @Override
    public String name() {
        return "FileUrlParser";
    }

    @Override
    public String description() {
        return "用于从httpUrl中解析文件数据，文件UrlElement列表中，每个文件需提供文件名、文件大小、以及获取文件流的httpUrl地址参数";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        return null;
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary, SparkSession sparkSession) {
        return null;
    }

    public Dataset<Row> toSparkDataFrame(List<UrlElement> urlElementList) {
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("fileName", DataTypes.StringType, true),
                DataTypes.createStructField("size", DataTypes.LongType, true),
                DataTypes.createStructField("content", DataTypes.BinaryType, true)
        });
        List<BinaryFile> rddList = new ArrayList<>();
        for (UrlElement urlElement : urlElementList) {
            byte[] content;
            try {
                URL url = new URL(urlElement.getHttpUrl());
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                try (InputStream inputStream = conn.getInputStream();
                     ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        byteStream.write(buffer, 0, bytesRead);
                    }
                    content = byteStream.toByteArray();
                }
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            rddList.add(new BinaryFile(urlElement.getFileName(), urlElement.getSize(), content));
        }
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(FairdServer.spark.sparkContext());
        JavaRDD<Row> rowRDD = sc.parallelize(rddList)
                .map(file -> RowFactory.create(file.getFileName(), file.getSize(), file.getContent()));
        Dataset<Row> df = FairdServer.spark.createDataFrame(rowRDD, schema);
        return df;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    private class BinaryFile implements Serializable {

        String fileName;
        Long size;
        byte[] content;
    }
}
