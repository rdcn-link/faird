package cn.cnic.demo.instdb;

import cn.cnic.faird.FairdServer;

/**
 * @author yaxuan
 * @create 2023/10/28 13:47
 */
public class InstDbServer {

    public static void main(String[] args) {

        FairdServer.create("instDb Server", "localhost", 3101, "cn.cnic.demo.instdb.InstDbService");
    }
}
