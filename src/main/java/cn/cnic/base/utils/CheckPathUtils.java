package cn.cnic.base.utils;

import org.slf4j.Logger;

import java.io.File;

public class CheckPathUtils {

	/**
     * Introducing logs, note that they are all packaged under "org.slf4j"
     */
    private static Logger logger = LoggerUtil.getLogger();

    public static void isChartPathExist(String dirPath) {
        File file = new File(dirPath);
        if (!file.exists()) {
            boolean mkdirs = file.mkdirs();
            if(!mkdirs){
                logger.warn("Create failed");
            }
        }
    }

    /**
     * 判断文件是否存在
     * @param filePath
     * @throws Exception
     * @author leilei
     * @date 2022-10-20
     */
    public static void isChartFileExist(String filePath) throws Exception {
        File file = new File(filePath);
        if (!file.exists()) {
            boolean mkFile = file.createNewFile();
            if(!mkFile){
                logger.warn("Create failed");
            }
        }
    }
}
