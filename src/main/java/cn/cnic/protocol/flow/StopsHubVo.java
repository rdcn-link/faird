package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class StopsHubVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String mountId;
    //private List<ThirdStopsComponentVo> stops;
    private String parentMountId;       //父mountId,和协同算法的jar包相关的字段

}
