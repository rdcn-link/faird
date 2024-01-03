package cn.cnic.protocol.flow;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class Path implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String from;
    private String outport;
    private String inport;
    private String to;
    private String filterCondition;

    public Path(String from,String to,String inport,String outport){
        this.from = from;
        this.to = to;
        this.inport = inport;
        this.outport = outport;
    }
}
