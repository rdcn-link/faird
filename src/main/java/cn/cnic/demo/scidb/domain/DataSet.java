package cn.cnic.demo.scidb.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.ivy.core.module.descriptor.License;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Getter
@Setter
@ToString
@Data
//@Document("v4_dataset")
public class DataSet implements Serializable {

    private static final long serialVersionUID = 1L;

    //@Id
    private String id;

    private String dataSetId;

    private String dataSetType;

    /**
     * public static final String PERSONAL = "p"; public static final String JOURANL = "j"; public static final String
     * ORGANIZATION = "o"; public static final String PROJECT = "r";
     **/
    private String dataSetTypeCode; // dataSetCode

    private String onLineUrl;// copyright;上传方式在线服务url 弃用字段 融合老版本数据用

	private List<String> correspondent;// 通讯作者信息 跟作者List产生关联

    private String titleZh;// 数据集标题(Zh)

    private String titleEn;// 数据集标题(En)

    private List<Author> author;// 数据集作者(中英双语) 一个 作者多机构 机构排序

    private List<Taxonomy> taxonomy;// 学科分类(枚举) 对齐 只支持国标 扩展成对象

    private List<String> categoryLabels; // 分类标签

    private List<String> keywordZh;// 关键字(Zh) 扩展成数组

    private List<String> keywordEn;// 关键字(En) 扩展成数组

    private String introductionZh;// 数据集简介(Zh)

    private String introductionEn;// 数据集简介(En)

    private List<Funding> funding;// 基金信息 扩展为数组 code string

    private List<String> referenceLink;// 数据集引用参考链接 扩展为数组

    private License copyRight;// 许可协议(枚举)

    /**
     * 001  dataset 数据集/文件集
     * 002  figure or table in publications  论文图表
     * 003  multimedia data  多媒体数据
     * 004  code data  代码数据
     * 005  slides  幻灯片
     */

    private String fileType;// 数据集文件类型(枚举)

    private String doi;// 数据集标识(自动生成)

    private String pid;// 数据集标识(自动生成)

    private String cstr;// 数据集标识(自动生成)

    private String username; // 数据集创建者(自动生成)

    private String version;// 数据集版本号(自动生成)
    /*
    public static final class DataSetStatus {
    
        // 文件关联状态
        public static final String FILEIMPORT = "-1";
        //新版本生成中
        public static final String FILEIMPORT = "-2";
        // 草稿状态
        public static final String DRAFT = "0";
        // 待审核状态
        public static final String PENDING_REVIEW = "1";
        // 审核通过状态
        public static final String AUDIT_COMPLETED = "2";
        // 发布状态
        public static final String PUBLISH = "3";
        // 返修中
        public static final String REVISE = "4";
    
    }
    */
    private String status;// 数据集状态(枚举)

    // 稳定状态 "0";
    // 非稳定状态  "-1";
    private String fileStatus;// 数据集文件状态(枚举)

    private Date dataSetCreateDate;// 数据集创建时间(自动生成)

    private Date dataSetUpdateDate;// 数据集更新时间(自动生成)

    private Date dataSetPublishDate;// 数据集发布时间(自动生成)

    private Date versionCreateDate;// 版本创建时间(自动生成)

    private Date versionUpdateDate;// 版本更新时间(自动生成)

    private Date versionPublishDate;// 版本发布时间(自动生成)

    private Date archiveDate;// 归档合sdb数据在ScienceDB上的发布时间

    private Date auditDate;// 审核时间

    private Date submitDate;// 数据集提交时间

    private long size;// 数据量 Byte double转为long

    private int count;// 文件数

    private int record;// 记录数

    private String shortUrl; // 短连接

    private String coverUrl;// 图片路径

    //@Transient
    private String coverBase64;// 图片转码

    private String language;// 数据集语言(默认支持中英双语) zh_CN, en_US

    /*
    未注册
    上传成功
    上传失败
    处理中
    校验异常
    已完成
    */
    private String doiStatus = "未注册";// DOI注册状态

    private Retraction retractions; // 撤稿信息

    private List<Paper> papers;// 关联论文

    private String source;// 数据来源 sdb-v3 sdb-v4 zenodo data-space archive

    private String publisher;// 出版商

    private String shareStatus;// 数据集共享方式 datasetStatus
    
	private String privateState; // 私有草稿状态 0非私有 1私有

    private String dataAuditor;// 数据审核人
    
    private int protectMonth; // 数据集保护期 月数

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm")
    @JsonFormat(timezone = "GMT+8",pattern = "yyyy-MM-dd HH:mm")
    private Date protectDay; // 数据集保护期 具体到天

	private List<String> topicList;// 专题唯一id

	private Set<String> indexedBy; //社区收录

	private String curatedBy;// 社区
	
	private String restricted;// 限制性获取中文

	private String restrictedEn;// 限制性获取英文

}