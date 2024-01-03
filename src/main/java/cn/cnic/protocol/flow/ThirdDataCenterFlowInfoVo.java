package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ThirdDataCenterFlowInfoVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String pid;
    private String name;
    private String state;
    private String startTime;
    private String endTime;
    private String progress;
    private String dataCenter;

    List<ThirdDataCenterFlowInfoStopKeyVo> stops = new ArrayList<>();

}
