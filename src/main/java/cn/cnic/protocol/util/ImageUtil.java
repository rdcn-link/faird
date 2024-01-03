package cn.cnic.protocol.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author yaxuan
 * @create 2023/12/14 12:04
 */
public class ImageUtil {

    public static byte[] getImage(String imagePath) {
        // 读取图片文件
        File file = new File(imagePath);
        try (FileInputStream fis = new FileInputStream(file);
             ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = fis.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
            byte[] imageBytes = bos.toByteArray();
            return imageBytes;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
