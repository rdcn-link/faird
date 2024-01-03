package cn.cnic.protocol.model;

import cn.cnic.protocol.bean.Group;
import cn.cnic.protocol.bean.PropertyDescriptor;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author yaxuan
 * @create 2023/12/14 09:45
 */
public abstract class AbstractParser implements Serializable {

    public abstract String getName();

    public abstract String getDescription();

    public abstract String getLanguage();

    public abstract String getAuthorEmail();

    public abstract Date getDateCreated();

    public abstract List<Group> getGroup();

    public abstract byte[] getIcon();

    public abstract String getExample();

    public abstract List<PropertyDescriptor> getPropertyDescriptor();

    public abstract void setProperties(Map<String, Object> map);

    public abstract void initialize();

    public abstract DataFrame parse(byte[] content);
}
