package cn.cnic.protocol.flow;

import cn.cnic.base.utils.ThreadLocalUtils;
import cn.cnic.base.utils.UUIDUtils;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class Flow implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String uuid;
    private String driverMemory = "1g";
    private String executorNumber = "1";
    private String executorMemory = "1g";
    private String executorCores = "1";
    private String description;
    private List<Stop> stopsList = new ArrayList<>();
    private List<Path> pathsList = new ArrayList<>();

    public Flow(String name) {
        this.uuid = UUIDUtils.getUUID32();
        this.name = name;
    }

    public Flow(String name,String description,String driverMemory,String executorNumber,String executorMemory,String executorCores){
        this.uuid = UUIDUtils.getUUID32();
        this.name = name;
        this.description = description;
        this.driverMemory = driverMemory;
        this.executorNumber = executorNumber;
        this.executorMemory = executorMemory;
        this.executorCores = executorCores;
    }

    public Flow addStop(String name, String dataframeId) {
        Stop stop = new Stop();
        stop.setName(name);
        stop.setBundel("cn.piflow.bundle.faird.ReadFaird");
        stop.setNodeType(StopType.STOP);
        List<Property> propertyList = new ArrayList<>();
        propertyList.add(new Property("serviceIp", ThreadLocalUtils.getConnect().getIp()));
        propertyList.add(new Property("servicePort", String.valueOf(ThreadLocalUtils.getConnect().getPort())));
        propertyList.add(new Property("dataframeId", dataframeId));
        stop.setProperty(propertyList);
        stopsList.add(stop);
        return this;
    }

    /**
     * 本地模型算法添加到flow（需要在这里配置本地模型算法的默认属性）
     * @param stop 本地模型算法
     * @return stop name
     * @throws Exception
     */
    public Flow addStop(Stop stop) {
        stop.setNodeType(StopType.STOP);
        List<Property> properties = stop.getProperties();
        if (null != properties && properties.size()>0) {
            List<Property> propertiesWithDefaultValue = properties.stream().map(pro -> {
                if(null == pro.getCustomValue() || "".equals(pro.getCustomValue())){
                    pro.setCustomValue(pro.getDefaultValue());
                }
                return pro;
            }).collect(Collectors.toList());
            stop.setProperties(propertiesWithDefaultValue);
        }
        stopsList.add(stop);
        return this;
    }

    public Flow addPath(Path path) {
        pathsList.add(path);
        return this;
    }
}
