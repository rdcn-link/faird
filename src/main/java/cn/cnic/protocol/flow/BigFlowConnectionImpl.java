package cn.cnic.protocol.flow;

import cn.cnic.base.utils.HttpUtils;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import net.sf.json.JSONObject;

import java.util.*;

@Getter
public class BigFlowConnectionImpl implements BigFlowConnection {

    private String url;
    private String token;

    public BigFlowConnectionImpl(String url, String token) {
        this.url = url;
        this.token = token;
    }

    @Override
    public Stop getLocalModel(String keyword) {
        Map<String, String> json = new HashMap<>();
        json.put("param", keyword);
        json.put("page", "1");
        json.put("limit", "1");
        Map<String, String> header = new HashMap<>();
        header.put("Authorization", "Bearer " + token);
        Map<String, String> headerParam = HttpUtils.setHeader(header);
        String response = HttpUtils.doPostFromComCustomizeHeader(url + "/piflow-web/stops/getStops", json, 10000, headerParam);
        List<Stop> localModels = JSON.parseArray(JSONObject.fromObject(response).getString("data"), Stop.class);
        return localModels.get(0);
    }
}
