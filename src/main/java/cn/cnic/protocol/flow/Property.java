
package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * stop property
 */
@Getter
@Setter
@NoArgsConstructor
public class Property implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String displayName;
    private String description;
    private String customValue;
    private String allowableValues;
    private Boolean required;
    private Boolean sensitive;
    private Boolean isSelect;
    private Boolean isLocked = false;
    private Long propertySort;
    private Boolean isOldData = false;
    private String example;
    private String defaultValue;
    private String leaderStopsId;

    public Property(String name, String customValue) {
        this.name = name;
        this.customValue = customValue;
    }
}
