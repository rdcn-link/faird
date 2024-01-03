package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProcessPathSDK {

    private static final long serialVersionUID = 1L;


    private String id;
    private String from;
    private String outport;
    private String inport;
    private String to;

}
