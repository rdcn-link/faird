package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class DatacenterBasisInfo implements Serializable {

    //平台网址
    private String platformSite;

    //依托单位
    private String supportingInstitution;

    //主管部门
    private String competentDepartment;

    //联系电话
    private String contactPhoneNumber;

    //联系邮箱
    private String contactEmail;

    //省市
    private String provincesCities;

    //图标地址
    private String iconAddress;

    //描述信息
    private String description;

}
