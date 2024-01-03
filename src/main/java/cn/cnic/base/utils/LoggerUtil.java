package cn.cnic.base.utils;

import cn.cnic.base.vo.ActionMethod;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class LoggerUtil {
    /**
     * Get the current "class" name, call this method directly in all places where the system uses "logger"
     *
     * @return
     */
    public static Logger getLogger() {
        StackTraceElement[] stackEle = new RuntimeException().getStackTrace();
        return LoggerFactory.getLogger(stackEle[1].getClassName());
    }

    public static void actionLogOk(ActionMethod method, String uri, String dataframeId, String query, long startTime) {
        ThreadContext.put("ip", ThreadLocalUtils.getConnect().getReqIp());
        ThreadContext.put("method", method.name());
        ThreadContext.put("uri", uri);
        if (!StringUtils.isEmpty(dataframeId)) {
            ThreadContext.put("dataframeId", dataframeId);
        }
        if (!StringUtils.isEmpty(query)) {
            ThreadContext.put("query", query);
        }
        long latency = System.currentTimeMillis() - startTime;
        ThreadContext.put("latency", latency + "ms");
        ThreadContext.put("status", "ok");
        log.info("This is a log message");
        ThreadContext.clearAll();
    }

    public static void actionLogFail(ActionMethod method, String uri, String dataframeId, String query, long startTime) {
        StackTraceElement[] stackEle = new RuntimeException().getStackTrace();
        Logger logger = LoggerFactory.getLogger(stackEle[1].getClassName());
        ThreadContext.put("ip", ThreadLocalUtils.getConnect().getReqIp());
        ThreadContext.put("method", method.name());
        ThreadContext.put("uri", uri);
        if (!StringUtils.isEmpty(dataframeId)) {
            ThreadContext.put("dataframeId", dataframeId);
        }
        if (!StringUtils.isEmpty(query)) {
            ThreadContext.put("query", query);
        }
        long latency = System.currentTimeMillis() - startTime;
        ThreadContext.put("latency", latency + "ms");
        ThreadContext.put("status", "fail");
        log.info("This is a log message");
        ThreadContext.clearAll();
    }
}