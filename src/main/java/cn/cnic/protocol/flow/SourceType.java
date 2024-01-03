package cn.cnic.protocol.flow;

import cn.cnic.base.vo.TextureEnumSerializer;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.StringUtils;

/**
 * @author yaxuan
 * @create 2023/3/17 15:18
 */
@JsonSerialize(using = TextureEnumSerializer.class)
public enum SourceType {
    COLLABORATIVE_NETWORK("协同网络", "COLLABORATIVE_NETWORK"),
    SEMANTIC_NETWORK("语义网", "SEMANTIC_NETWORK");

    private final String value;
    private final String text;

    private SourceType(String text, String value) {
        this.text = text;
        this.value = value;
    }

    public String getText() {
        return text;
    }

    public String getValue() {
        return value;
    }

    public static SourceType selectGender(String name) {
        for (SourceType addNetWork : SourceType.values()) {
            if (name.equalsIgnoreCase(addNetWork.name())) {
                return addNetWork;
            }
        }
        return null;
    }

    public static SourceType selectGenderByValue(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        for (SourceType portType : SourceType.values()) {
            if (value.equalsIgnoreCase(portType.value)) {
                return portType;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        JSONObject object = new JSONObject();
        object.put("stringValue", value);
        object.put("text", text);
        return object.toString();
    }
}

