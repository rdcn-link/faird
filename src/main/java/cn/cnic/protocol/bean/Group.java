package cn.cnic.protocol.bean;

import cn.cnic.base.vo.TextureEnumSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author yaxuan
 * @create 2023/12/14 11:59
 */
@JsonSerialize(using = TextureEnumSerializer.class)
public enum Group {

    CsvGroup("CSV","CSV"),
    JsonGroup("JSON", "JSON"),
    XmlGroup("XML", "XML");

    private final String value;
    private final String text;

    Group(String text, String value) {
        this.text = text;
        this.value = value;
    }
}
