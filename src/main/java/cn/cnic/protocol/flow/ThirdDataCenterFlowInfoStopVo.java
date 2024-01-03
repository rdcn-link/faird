package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class ThirdDataCenterFlowInfoStopVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private String state;
    private String startTime;
    private String endTime;
    private String flowId;
    private String dataCenter;

    private String dataCenterId;            //计算节点id(通过请求server端返回的dataCenter,获取对应的dataCenterId和dataCenterName,是在web端拼接的)
    private String dataCenterName;          //计算节点name(同上)

}
