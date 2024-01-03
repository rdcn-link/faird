package cn.cnic.base.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author yaxuan
 * @create 2023/9/4 16:06
 */
public class SparkUtils {

    public static void addToZip(File file, String parentDir, ZipOutputStream zipOutputStream) throws IOException {
        if (file.isDirectory()) {
            String dirPath = parentDir + file.getName() + File.separator;
            zipOutputStream.putNextEntry(new ZipEntry(dirPath));
            File[] files = file.listFiles();
            for (File nestedFile : files) {
                addToZip(nestedFile, dirPath, zipOutputStream);
            }
        } else {
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                zipOutputStream.putNextEntry(new ZipEntry(parentDir + file.getName()));
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                    zipOutputStream.write(buffer, 0, bytesRead);
                }
            }
        }
    }

    public static String transSqlCode(String sqlCode) {
        sqlCode = sqlCode.replace(";", "").replace("\"", "'").replace("\\n", "\n");
        Pattern p = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|//.*?$|/\\*.*?\\*/|#.*?$|");
        sqlCode = p.matcher(sqlCode).replaceAll("$1").replace("\n", " ");
        // 中文字符
        String[] splits = sqlCode.split(" ");
        StringBuilder sb = new StringBuilder();
        for (String split : splits) {
            if (containsChineseCharacter(split)) {
                sb.append("`").append(split).append("` ");
            } else {
                sb.append(split).append(" ");
            }
        }
        return sb.toString();
    }

    public static String extractTableName(String sql) {
        // Pattern to match table name after FROM keyword
        String pattern = "\\bFROM\\s+(\\w+)";
        Pattern regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = regex.matcher(sql);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    public static boolean containsChineseCharacter(String inputStr) {
        String regex = "[\\u4E00-\\u9FFF]+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(inputStr);
        return matcher.find();
    }
}
