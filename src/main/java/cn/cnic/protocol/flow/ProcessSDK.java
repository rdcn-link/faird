package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Getter
@Setter
public class ProcessSDK {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String driverMemory;
    private String executorNumber;
    private String executorMemory;
    private String executorCores;
    private String description;
    private String flowId;
    private String appId;
    private ProcessState state;
    private Date startTime;
    private Date endTime;
    private String progress;
    private RunModeType runModeType = RunModeType.RUN;
    private ProcessParentType processParentType;
    private List<ProcessStopSDK> processStopList = new ArrayList<>();
    private List<ProcessPathSDK> processPathList = new ArrayList<>();
    //private List<FlowGlobalParams> flowGlobalParamsList;

}
