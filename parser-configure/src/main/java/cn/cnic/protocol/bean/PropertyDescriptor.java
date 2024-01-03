package cn.cnic.protocol.bean;

import java.io.Serializable;
import java.util.Set;

/**
 * @author yaxuan
 * @create 2023/12/14 10:40
 */
public class PropertyDescriptor implements Serializable {

    private String name;
    private String description;
    private String defaultValue;
    private Set<String> allowableValues;
    private boolean required = false;
    private boolean sensitive = true;
    private String example;
    private PropertyLanguage language = PropertyLanguage.TEXT;

    public PropertyDescriptor name(String name) {
        this.name = name;
        return this;
    }

    public PropertyDescriptor description(String description) {
        this.description = description;
        return this;
    }

    public PropertyDescriptor defaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public PropertyDescriptor allowableValues(Set<String> allowableValues) {
        this.allowableValues = allowableValues;
        return this;
    }

    public PropertyDescriptor required(boolean required) {
        this.required = required;
        return this;
    }

    public PropertyDescriptor sensitive(boolean sensitive) {
        this.sensitive = sensitive;
        return this;
    }

    public PropertyDescriptor example(String example) {
        this.example = example;
        return this;
    }

    public PropertyDescriptor language(PropertyLanguage language) {
        this.language = language;
        return this;
    }
}
