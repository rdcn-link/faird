package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@Data
//@Document("v4_dataset_paper")
@ToString
public class Paper implements Serializable {

    private static final long serialVersionUID = 1L;
    private String titleZh;// 论文中文标题
    private String titleEn;// 论文英文标题
    private String doi;// 论文DOI
    private String url;// 论文链接
    private String citationZh; // 关联出版论文信息
    private String citationEn; // 关联出版论文信息

    // 状态 稿件编号 期刊名称
    /*
            草稿: manuscript
            状态：已经录用
            已出版:published
     */
    private String state;
    private String manuscriptNo;// 稿件编号
    private String journalZh;// 期刊名称
    private String journalEn;// 期刊英文名称
    private String journalCode;// 期刊code
}
