package cn.cnic.protocol.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author yaxuan
 * @create 2023/10/28 10:35
 */
public interface Parser extends Serializable {

    String name();

    String description();

    Dataset<Row> toSparkDataFrame(byte[] binary);

    Dataset<Row> toSparkDataFrame(byte[] binary, SparkSession sparkSession);
}
