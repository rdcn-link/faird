package cn.cnic.base.utils;

import cn.cnic.faird.FairdConnectionImpl;

/**
 * @author yaxuan
 * @create 2023/11/1 13:52
 */
public class ThreadLocalUtils {

    private static final ThreadLocal<FairdConnectionImpl> connectionThreadLocal = new ThreadLocal<>();

    public static void addConnection(FairdConnectionImpl connection) {
        connectionThreadLocal.set(connection);
    }

    /**
     * 获取当前connection
     */
    public static FairdConnectionImpl getConnect() {
        return connectionThreadLocal.get();
    }


    /**
     * 删除当前connection
     */
    public static void remove() {
        connectionThreadLocal.remove();
    }
}
