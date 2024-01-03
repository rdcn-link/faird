package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Getter
@Setter
@ToString
@Data
//@Document("v4_dataset_author")
public class Author implements Serializable {

    private static final long serialVersionUID = 1L;
    private String id;

    private String nameZh;
    private String nameEn;
    private String orcId;
    private List<Organization> organizations;// 机构
    private String email;
    private String ucstr;
    private Set<String> country_name;

}
