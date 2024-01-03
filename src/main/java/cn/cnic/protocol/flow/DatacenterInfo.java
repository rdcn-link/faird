package cn.cnic.protocol.flow;

import lombok.Data;
import lombok.ToString;
import net.sf.json.JSONObject;

import java.io.Serializable;
import java.util.List;

/**
 * 数据中心描述信息
 *
 * @author SongDz
 * @date 2022/6/14 9:29
 **/
@Data
@ToString
public class DatacenterInfo extends DatacenterBasisInfo implements Serializable {

    /**
     * 数据中心id
     */
    private String id;
    /**
     * 数据中心名称
     */
    private String name;
    /**
     * 数据中心描述
     */
//    private String description;
    /**
     * 数据中心piflow-web地址 ip+port
     */
    private String webUrl;
    /**
     * 数据中心piflow-server地址 ip+port
     */
    private String computeNodeUrl;
    /**
     * 本地时间戳版本信息，记录最后修改时间（worker节点生成）
     */
    private long timestampVersion;
    /**
     * 数据中心是否是leader节点
     */
    private boolean isLeader = false;
    /**
     * 数据中心第一次登录到leader的时间（leader生成）
     */
    private long firstLoginTime;
    /**
     * 数据中心最近一次登录到leader的时间（leader生成）
     */
    private long lastLoginTime;
    /**
     * 数据中心第一次登录到leader的ip（leader生成）
     */
    private long firstLoginIp;
    /**
     * 数据中心最近一次登录到leader的ip（leader生成）
     */
    private long lastLoginIp;
    /**
     * leader节点最大时间戳版本信息（leader生成）
     */
    //private long leaderMaxTimestampVersion;

    private JSONObject resourceInfo;

    //private List<DataSourceVersionVo> dataSourceVersions; //dataSource id, inner_version

    //private List<DataSourceVersionVo> stopHubVersions; //stopHub id, inner_version

    private List<StopsHubVo> stopsHubVoList;        //server端plugin表的id和parentMountId(此数据用于协同算法流水线拼接时,判断哪个节点有此jar包)
}