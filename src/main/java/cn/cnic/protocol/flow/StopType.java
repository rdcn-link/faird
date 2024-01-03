package cn.cnic.protocol.flow;

import cn.cnic.base.vo.TextureEnumSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.StringUtils;

/**
 * @author yaxuan
 * @create 2023/3/17 15:18
 */
@JsonSerialize(using = TextureEnumSerializer.class)
public enum StopType {
    STOP("Stop", "Stop"),
    DATASOURCE("DataSource", "DataSource"),
    COMMONLYUSEDDATASOURCE("CommonlyUsedDataSource","CommonlyUsedDataSource"),
    COMMONLYUSEDSTOP("CommonlyUsedStop","CommonlyUsedStop"),
    CUSTOMIZESTOP("CustomizeStop","CustomizeStop");     //自定义算法(mount的算法包)


    private final String value;
    private final String text;

    private StopType(String text, String value) {
        this.text = text;
        this.value = value;
    }

    public String getText() {
        return text;
    }

    public String getValue() {
        return value;
    }

    public static StopType selectGender(String name) {
        for (StopType stopType : StopType.values()) {
            if (name.equalsIgnoreCase(stopType.name())) {
                return stopType;
            }
        }
        return null;
    }

    public static StopType selectGenderByValue(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        for (StopType portType : StopType.values()) {
            if (value.equalsIgnoreCase(portType.value)) {
                return portType;
            }
        }
        return null;
    }
}
