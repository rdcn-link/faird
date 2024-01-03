
package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * stop property
 */
@Getter
@Setter
public class CustomizedPropertySDK implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String customValue;
    private String description;
}
