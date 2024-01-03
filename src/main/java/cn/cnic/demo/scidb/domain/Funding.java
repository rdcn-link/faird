package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@Data
//@Document("v4_funding")
public class Funding implements Serializable {
    private static final long serialVersionUID = 1L;
    private String type;// v4 or v3
    private String funding_nameZh;
    private String funding_nameEn;
    private String funding_code;
	private String funding_id;
}
