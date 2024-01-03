package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ThirdDataCenterFlowInfoAllVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String state;
    private String startTime;
    private String endTime;
    private String progress;
    private List<String> groups = new ArrayList<>();
    private List<ThirdDataCenterFlowInfoKeyVo> flows = new ArrayList<>();

}
