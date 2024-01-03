package cn.cnic.fairdrpc;

import cn.cnic.base.utils.UUIDUtils;
import cn.cnic.faird.FairdConnection;
import cn.cnic.protocol.model.AbstractParser;
import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.bean.PropertyDescriptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author yaxuan
 * @create 2023/12/18 17:09
 */
public class TestParser {

    @Test
    void TestCsvParser() throws Exception {
        FairdConnection connect = FairdConnection.connect("10.0.82.71", 3101, "");
        String uri = "fair://10.0.82.71/eaa5d25a5b1549a08b49b45bb0ed33ff";
        List<DataFrame> dataFrameList = connect.model(uri);
        DataFrame df = dataFrameList.get(0);
        // 获取parser的属性描述
        Path jarFilePath = downloadJar("6581a01aaf305554b98dcbe1");
        List<AbstractParser> parserList = loadJar(jarFilePath.toFile());
        AbstractParser parser = parserList.get(0);
        List<PropertyDescriptor> propertyDescriptorList = parser.getPropertyDescriptor();
        // 自定义属性
        Map<String, Object> properties = new HashMap<>();
        properties.put("header", true);
        properties.put("delimiter", ",");
        DataFrame csvDf = df.open(0, "6581a01aaf305554b98dcbe1", properties);
        String s = csvDf.printData(10, 20);
        System.out.println("****");
    }

    Path downloadJar(String parserId) {
        // 创建临时文件
        Path jarTempFilePath;
        try {
            jarTempFilePath = Files.createTempFile("temp-jar-" + UUIDUtils.getUUID32(), ".jar");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet("http://10.0.82.94:5000/download?_id=" + parserId);
        try {
            InputStream responseStream = httpClient.execute(httpGet).getEntity().getContent();
            FileOutputStream outputStream = new FileOutputStream(jarTempFilePath.toFile());
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = responseStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.close();
            responseStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jarTempFilePath;
    }

    List<AbstractParser> loadJar(File jarFile) {
        List<AbstractParser> parserList = new ArrayList<>();
        try {
            String dependencyPath = "/Users/yaxuan/idea_workspace/faird-rpc/parser-configure/target/parser-configure-0.0.1-SNAPSHOT.jar";
            URL jarUrl = jarFile.toURI().toURL();
            URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl, new File(dependencyPath).toURI().toURL()});
            ZipInputStream zipInputStream = new ZipInputStream(jarUrl.openConnection().getInputStream());
            ZipEntry entry = null;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    if (entry.getName().endsWith(".class")) {
                        String entryName = entry.getName().substring(0, entry.getName().length() - 6).replaceAll("/", ".");
                        try {
                            Class<?> clazz = classLoader.loadClass(entryName);
                            Class<?> superclass = clazz.getSuperclass();
                            if (superclass.getName().equals("cn.cnic.protocol.model.AbstractParser")) {
                                AbstractParser parser = (AbstractParser) clazz.newInstance();
                                parserList.add(parser);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parserList;
    }
}
