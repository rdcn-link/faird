package cn.cnic.demo.scidb;

import cn.cnic.faird.FairdServer;

/**
 * @author yaxuan
 * @create 2023/11/3 18:01
 */
public class SciDbServer {

    public static void main(String[] args) {

        FairdServer.create("sciDb Server", "localhost", 3102, "cn.cnic.demo.scidb.SciDbService");
    }
}
