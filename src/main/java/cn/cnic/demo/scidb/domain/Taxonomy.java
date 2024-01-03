package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
//@Document("v4_taxonomy")
@Data
public class Taxonomy implements Serializable {
    private static final long serialVersionUID = 1L;
    private String code;
    private String nameZh;
    private String nameEn;
}
