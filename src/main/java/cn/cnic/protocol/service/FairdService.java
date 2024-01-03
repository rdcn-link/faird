package cn.cnic.protocol.service;

import cn.cnic.protocol.model.DataFrame;
import cn.cnic.protocol.model.MetaData;
import cn.cnic.protocol.vo.UrlElement;

import java.util.List;

/**
 * @author yaxuan
 * @create 2023/10/28 13:49
 */
public interface FairdService {

    MetaData getMeta(String identifier);

    List<DataFrame> getData(String identifier);

}
