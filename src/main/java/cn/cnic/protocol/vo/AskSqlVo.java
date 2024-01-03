package cn.cnic.protocol.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author yaxuan
 * @create 2023/11/8 10:25
 */
@Getter
@Setter
@AllArgsConstructor
public class AskSqlVo implements Serializable {

    private String id;
    private String sqlQuery;
}
