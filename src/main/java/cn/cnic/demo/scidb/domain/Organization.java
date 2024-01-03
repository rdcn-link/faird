package cn.cnic.demo.scidb.domain;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
@Data
//@Document("v4_organization")
public class Organization implements Serializable {
    private static final long serialVersionUID = 1L;

    private String nameZh;
    private String nameEn;
	private String id;
	private String custom;
	private List<OrgIds> orgids;
	private List<String> aliases;
    private int order;

    @Override
    public boolean equals(Object obj) {
        if(this == obj){
            return true;//地址相等
        }

        if(obj == null){
            return false;//非空性：对于任意非空引用x，x.equals(null)应该返回false。
        }

        if(obj instanceof Organization){
            Organization other = (Organization) obj;
            //需要比较的字段相等，则这两个对象相等
            if(equalsStr(this.nameZh, other.nameZh)
                    && equalsStr(this.nameEn, other.nameEn)){
                return true;
            }
        }

        return false;
    }

    private boolean equalsStr(String str1, String str2){
        if(StringUtils.isEmpty(str1) && StringUtils.isEmpty(str2)){
            return true;
        }
        if(!StringUtils.isEmpty(str1) && str1.equals(str2)){
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (nameZh == null ? 0 : nameZh.hashCode());
        result = 31 * result + (nameEn == null ? 0 : nameEn.hashCode());
        return result;
    }
}
