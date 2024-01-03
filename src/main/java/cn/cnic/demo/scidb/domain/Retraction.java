package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
//@Document("v4_dataset_retraction")
@Data
public class Retraction implements Serializable {

    private static final long serialVersionUID = 1L;
    // 撤稿信息中文
    private String informationZh;
    // 撤稿信息英文
    private String informationEn;

}
