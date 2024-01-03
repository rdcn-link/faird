package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class ThirdDataCenterFlowInfoKeyVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private ThirdDataCenterFlowInfoVo flow;

}
