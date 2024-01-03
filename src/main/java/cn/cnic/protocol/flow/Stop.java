package cn.cnic.protocol.flow;

import lombok.Data;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class Stop implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String bundel;
    private String groups;
    private String owner;
    private String description;
    private String inports;
    private PortType inPortType;
    private String outports;
    private PortType outPortType;
    private String state;
    private Date startTime;
    private Date stopTime;
    private Boolean isCheckpoint;
    private Boolean isCustomized = false;
    private List<Property> properties = new ArrayList<>();
    private List<Property> oldProperties = new ArrayList<>();
    private List<CustomizedPropertySDK> customizedPropertyList = new ArrayList<>();
    private String dataCenterId;
    private String dataCenterName;
    private String fkDataSourceId;

    private String dataCenter;            //数据中心
    private String webAddress;            //数据中心地址
    private StopType nodeType;                //节点类型(Stop/DataSource/CommonlyUsedDataSource/CommonlyUsedStop)
    private SourceType sourceType;           //常用协同数据源来源(协同数据源来自语义网还是协同网络)
    private ComponentFileType componentType;        //算法类型(不保存到表中)
    private String dockerImagesName;          //python组件生成的docker镜像地址(此字段不保存到表中,作为临时字段每次需要去数据库查,虽然有路径了,但路径保存的是压缩包,这里要的是压缩包里文件的名字)

    /**
     * Set property value
     * @param properties->Map<propertyName,propertyValue>
     * @return
     */
    public boolean setProperty(Map<String,String> properties){
        //get properties which no need to modify
        List<Property> propertyList = this.properties.stream().filter(property -> !properties.containsKey(property.getName())).collect(Collectors.toList());
        List<Property> modifyPropertylist = properties.keySet().stream().map(key -> {
            Property newProperty = new Property();
            Optional<Property> propertyOptional = this.properties.stream().filter(pro -> pro.getName().equals(key)).findFirst();
            if (propertyOptional.isPresent()) {
                newProperty = propertyOptional.get();
                newProperty.setCustomValue(properties.get(key));
            }
            return newProperty;
        }).filter(property -> StringUtils.isNotBlank(property.getName())).collect(Collectors.toList());
        propertyList.addAll(modifyPropertylist);
        this.properties = propertyList;
        return true;
    }

    public boolean setProperty(List<Property> properties) {
        this.properties = properties;
        return true;
    }

    public void setInPortType(String inPortType){
        String value = (String) JSONObject.fromObject(inPortType).get("stringValue");
        this.inPortType = PortType.selectGenderByValue(value);
    }
    public void setOutPortType(String outPortType){
        String value = (String) JSONObject.fromObject(outPortType).get("stringValue");
        this.outPortType = PortType.selectGenderByValue(value);
    }
    public void setStopType(String stopType){
        String value = (String) JSONObject.fromObject(stopType).get("stringValue");
        this.nodeType = StopType.selectGenderByValue(value);
    }
    public void setSourceType(String sourceType){
        String value = (String) JSONObject.fromObject(sourceType).get("stringValue");
        this.sourceType = SourceType.selectGenderByValue(value);
    }
    public void setComponentType(String componentType){
        String value = (String) JSONObject.fromObject(componentType).get("stringValue");
        this.componentType = ComponentFileType.selectGenderByValue(value);
    }
}
