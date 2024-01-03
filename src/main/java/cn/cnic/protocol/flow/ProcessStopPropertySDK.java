package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProcessStopPropertySDK implements Comparable<ProcessStopPropertySDK>{

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String displayName;
    private String description;
    private String customValue;
    private String allowableValues;
    private Boolean required;
    private Boolean sensitive;
    private Long propertySort;

    @Override
    public int compareTo(ProcessStopPropertySDK o) {
        return this.propertySort.compareTo(o.propertySort);
    }
}
