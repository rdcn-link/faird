package cn.cnic.protocol.flow;

import cn.cnic.base.utils.HttpUtils;
import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public interface BigFlowConnection {

    static BigFlowConnectionImpl Connect(String url, String username, String password) {
        Integer timeOutMS = 10000;
        Map<String, String> json = new HashMap<>();
        json.put("username", username);
        json.put("password", password);
        Map<String, String> herderParam = HttpUtils.setHeader(null);
        String doPostComCustomizeHeader = HttpUtils.doPostFromComCustomizeHeader(url + "/piflow-web/jwtLogin", json, timeOutMS, herderParam);
        String token = JSONObject.fromObject(doPostComCustomizeHeader).getString("token");
        BigFlowConnectionImpl bigFlowConnection = new BigFlowConnectionImpl(url, token);
        return bigFlowConnection;
    }

    Stop getLocalModel(String keyword);
}
