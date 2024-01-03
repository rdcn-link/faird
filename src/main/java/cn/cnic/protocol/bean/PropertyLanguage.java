package cn.cnic.protocol.bean;

import cn.cnic.base.vo.TextureEnumSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author yaxuan
 * @create 2023/12/14 10:44
 */
@JsonSerialize(using = TextureEnumSerializer.class)
public enum PropertyLanguage {

    TEXT("TEXT","TEXT"),
    SCALA("SCALA", "SCALA"),
    PYTHON("PYTHON", "PYTHON"),
    SHELL("SHELL", "SHELL"),
    SQL("SQL", "SQL");

    private final String value;
    private final String text;

    PropertyLanguage(String text, String value) {
        this.text = text;
        this.value = value;
    }
}
