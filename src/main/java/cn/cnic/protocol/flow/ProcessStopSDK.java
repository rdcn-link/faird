package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Setter
@Getter
public class ProcessStopSDK {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String bundel;
    private String groups;
    private String owner;
    private String description;
    private String inports;
    private PortType inPortType;
    private String outports;
    private PortType outPortType;
    private StopState state = StopState.INIT;
    private Date startTime;
    private Date endTime;
    private List<ProcessStopPropertySDK> processStopPropertyList = new ArrayList<>();
    private List<ProcessStopCustomizedPropertySDK> processStopCustomizedPropertyList = new ArrayList<>();

    private String dataSourceId;            //数据源id(可能是本地数据源id,也可能是常用数据源id)
    private String dataCenterId;            //计算节点id
    private String dataCenterName;          //计算节点name
    private StopType nodeType;                //节点类型(Stop/DataSource/CommonlyUsedDataSource/CommonlyUsedStop)
    private String mountId;                 //协同算法的jar包的mountId(不保存到表中)

    private String dockerImagesName;          //python组件生成的docker镜像地址(此字段不保存到表中,作为临时字段每次需要去数据库查,虽然有路径了,但路径保存的是压缩包,这里要的是压缩包里文件的名字)
    private SourceType sourceType;           //常用协同数据源来源(协同数据源来自语义网还是协同网络)
    private ComponentFileType componentType;        //算法类型(不保存到表中)

    private String dataCenter;            //数据中心
    private String webAddress;            //数据中心地址
}
