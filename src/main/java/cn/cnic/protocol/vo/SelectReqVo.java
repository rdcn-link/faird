package cn.cnic.protocol.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * @author yaxuan
 * @create 2023/11/7 15:16
 */
@Getter
@Setter
@AllArgsConstructor
public class SelectReqVo implements Serializable {

    private String id;
    private String field;
    private List<String> fields;
}
