package cn.cnic.protocol.parser;

import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.faird.FairdServer;
import cn.cnic.protocol.model.Parser;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author yaxuan
 * @create 2023/10/28 10:45
 */
public class FtpParser implements Parser {

    @Override
    public String name() {
        return "FtpParser";
    }

    @Override
    public String description() {
        return "用于从FTP服务器中获取文件数据，需提供ftp server地址、port端口、username、password参数";
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary) {
        return null;
    }

    @Override
    public Dataset<Row> toSparkDataFrame(byte[] binary, SparkSession sparkSession) {
        return null;
    }

    public Dataset<Row> toSparkDataFrame(String server, int port, String username, String password) {
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();
            // FTP文件转存本地
            String localDir = System.getProperty("user.dir") + "/storage/ftp/" + UUIDUtils.getUUID32();
            new File(localDir).mkdirs();
            ftpClient.changeWorkingDirectory("/");
            ftpClient.setControlEncoding("UTF-8");
            FTPFile[] ftpFiles = ftpClient.listFiles();
            for (FTPFile ftpFile : ftpFiles) {
                String fileName = ftpFile.getName();
                String localFilePath = localDir + "/" + fileName;
                if (ftpFile.isFile()) {
                    FileOutputStream fos = new FileOutputStream(localFilePath);
                    ftpClient.retrieveFile(fileName, fos);
                    fos.close();
                } else if (ftpFile.isDirectory()) {
                    createLocalDirectory(localFilePath);
                    FTPFile[] subFiles = ftpClient.listFiles( "/" + fileName);
                    copyFilesFromFTPToLocalStorage(ftpClient, subFiles, "/" + fileName, localFilePath);
                }
            }
            // 关闭FTP连接
            ftpClient.logout();
            ftpClient.disconnect();
            // 从本地创建dataframe
            Dataset<Row> df = FairdServer.spark.read().format("binaryFile").option("recursiveFileLookup", "true").load(localDir);
            return df;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void createLocalDirectory(String localDirectory) {
        File dir = new File(localDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    private void copyFilesFromFTPToLocalStorage(FTPClient ftpClient, FTPFile[] ftpFiles, String remoteDirectory, String localDirectory) throws IOException {
        for (FTPFile ftpFile : ftpFiles) {
            String fileName = ftpFile.getName();
            String localFilePath = localDirectory + "/" + fileName;
            if (ftpFile.isFile()) {
                FileOutputStream fos = new FileOutputStream(localFilePath);
                ftpClient.retrieveFile(remoteDirectory + "/" + fileName, fos);
                fos.close();
            } else if (ftpFile.isDirectory()) {
                createLocalDirectory(localFilePath);
                FTPFile[] subFiles = ftpClient.listFiles(remoteDirectory + "/" + fileName);
                copyFilesFromFTPToLocalStorage(ftpClient, subFiles, remoteDirectory + "/" + fileName, localFilePath);
            }
        }
    }
}
