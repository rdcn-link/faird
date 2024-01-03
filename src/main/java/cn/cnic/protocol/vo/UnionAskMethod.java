package cn.cnic.protocol.vo;

/**
 * @author yaxuan
 * @create 2023/10/30 17:50
 */
public enum UnionAskMethod {

    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN,
    CROSS_JOIN,
    UNION,
    UNION_ALL,
    INTERSECT,
    INTERSECT_ALL;
}
