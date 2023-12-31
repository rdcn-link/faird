package cn.cnic.protocol.flow;

import cn.cnic.base.vo.TextureEnumSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.StringUtils;

/**
 * @author yaxuan
 * @create 2023/3/17 15:15
 */
@JsonSerialize(using = TextureEnumSerializer.class)
public enum PortType {
    ANY("Any", "Any", "Any quantity"),
    DEFAULT("Default", "Default", "Default number 1"),
    USER_DEFAULT("USER_DEFAULT", "USER_DEFAULT", " 'stop' defines ports"),
    NONE("None", "None", "prohibit"),
    ROUTE("Route", "Route", "Routing port");

    private final String value;
    private final String text;
    private final String desc;

    private PortType(String text, String value, String desc) {
        this.text = text;
        this.value = value;
        this.desc = desc;
    }

    public String getText() {
        return text;
    }

    public String getValue() {
        return value;
    }

    public String getDesc() {
        return desc;
    }

    public static PortType selectGender(String name) {
        if (StringUtils.isBlank(name)) {
            return PortType.DEFAULT;
        }
        for (PortType portType : PortType.values()) {
            if (name.equalsIgnoreCase(portType.name())) {
                return portType;
            }
        }
        return null;
    }

    public static PortType selectGenderByValue(String value) {
        if (StringUtils.isBlank(value)) {
            return PortType.DEFAULT;
        }
        for (PortType portType : PortType.values()) {
            if (value.equalsIgnoreCase(portType.value)) {
                return portType;
            }
        }
        return null;
    }
}

